#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для получения данных о группах объявлений
Использует общие модули database_manager и api_client
"""

import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any

from database_manager import DatabaseManager
from api_client import DirectAPIClient
from minio_client import MinIOClient

class AdGroupsDataProcessor:
    """Обработчик для получения данных о группах объявлений"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = None
        self.current_account = None
        self.minio_client = MinIOClient()
    
    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск получения данных о группах объявлений")
        print("="*60)
        
        # Подключаемся к БД
        if not self.db.connect():
            return False
        
        try:
            # Получаем отчеты для обработки
            reports = self.db.get_reports_to_process()
            if not reports:
                print("ℹ️ Нет отчетов для обработки")
                return True
            
            # Получаем аккаунты Яндекс.Директ
            accounts = self.db.get_yandex_accounts()
            if not accounts:
                print("❌ Не найдены аккаунты Яндекс.Директ")
                return False
            
            # Обрабатываем каждый отчет
            for report in reports:
                print(f"\n📋 Обработка отчета ID: {report['id']}")
                self.process_single_report(report, accounts)
            
            return True
            
        finally:
            self.db.disconnect()
    
    def process_single_report(self, report: Dict, accounts: List[Dict]):
        """Обрабатывает один отчет"""
        try:
            # Получаем данные договора
            contract_data = self.db.get_contract_data(report['id_contracts'])
            if not contract_data:
                print(f"❌ Не найдены данные договора ID: {report['id_contracts']}")
                return
            
            # Получаем данные заявки
            request_data = self.db.get_request_data(report['id_requests'])
            if not request_data:
                print(f"❌ Не найдены данные заявки ID: {report['id_requests']}")
                return
            
            # Извлекаем ID кампаний
            campaign_ids = self.db.extract_campaign_ids(request_data.get('campany_yandex_direct'))
            if not campaign_ids:
                print("❌ Не найдены ID кампаний")
                return False
            
            print(f"📊 Найдено кампаний: {len(campaign_ids)}")
            print(f"📊 ID кампаний: {campaign_ids}")
            
            # Получаем удаленные группы для исключения
            deleted_group_ids = self.parse_deleted_groups(request_data)
            
            # Пытаемся получить данные с разными аккаунтами
            adgroups_data = None
            
            for account in accounts:
                print(f"\n🔑 Попытка с аккаунтом ID: {account['id']}")
                
                # Используем логин из договора, если он есть
                client_login = contract_data.get('login_yandex_direct')
                if client_login:
                    print(f"✅ Используем логин из договора: {client_login}")
                else:
                    print(f"⚠️ Логин из договора не найден, используем Client ID")
                    client_login = account.get('client_id')
                
                # Создаем API клиент
                self.api_client = DirectAPIClient(
                    account['direct_api_token'],
                    client_login
                )
                
                # Тестируем подключение
                if self.api_client.test_connection():
                    print("✅ Подключение к API успешно")
                    
                    # Получаем данные о группах
                    adgroups_data = self.get_adgroups_data(campaign_ids, deleted_group_ids)
                    if adgroups_data:
                        print("✅ Данные о группах получены успешно")
                        self.current_account = account
                        break
                    else:
                        print("❌ Не удалось получить данные о группах")
                else:
                    print("❌ Ошибка подключения к API")
                
                # Небольшая пауза между попытками
                time.sleep(2)
            
            if adgroups_data:
                # Сохраняем данные
                self.save_adgroups_data(adgroups_data, report)
            else:
                print("❌ Не удалось получить данные ни с одним аккаунтом")
                
        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")
    
    def parse_deleted_groups(self, request_data: Dict) -> List[int]:
        """Парсит удаленные группы из поля deleted_groups в формате JSON"""
        try:
            deleted_groups_data = request_data.get('deleted_groups')
            if not deleted_groups_data:
                print("ℹ️ Поле deleted_groups не найдено или пустое")
                return []
            
            # Если это строка, парсим JSON
            if isinstance(deleted_groups_data, str):
                deleted_groups_data = json.loads(deleted_groups_data)
            
            # Проверяем, что это словарь
            if not isinstance(deleted_groups_data, dict):
                print("⚠️ deleted_groups не является словарем")
                return []
            
            # Собираем все ID групп из всех кампаний
            all_deleted_group_ids = []
            for campaign_id, group_ids in deleted_groups_data.items():
                if isinstance(group_ids, list):
                    # Преобразуем в int и добавляем
                    for group_id in group_ids:
                        try:
                            all_deleted_group_ids.append(int(group_id))
                        except (ValueError, TypeError):
                            print(f"⚠️ Неверный ID группы: {group_id}")
                            continue
                    print(f"📋 Кампания {campaign_id}: исключаем {len(group_ids)} групп")
                else:
                    print(f"⚠️ Группы для кампании {campaign_id} не являются списком")
            
            if all_deleted_group_ids:
                print(f"🚫 Всего исключаем {len(all_deleted_group_ids)} групп: {all_deleted_group_ids[:10]}{'...' if len(all_deleted_group_ids) > 10 else ''}")
            else:
                print("ℹ️ Нет групп для исключения")
            
            return all_deleted_group_ids
            
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON deleted_groups: {e}")
            return []
        except Exception as e:
            print(f"❌ Ошибка обработки deleted_groups: {e}")
            return []
    
    def get_adgroups_data(self, campaign_ids: List[int], deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Получает данные о группах через API"""
        try:
            # Проверяем корректность campaign_ids
            if not campaign_ids:
                print("❌ Не переданы ID кампаний")
                return None
            
            # Проверяем, что все ID являются числами
            if not all(isinstance(cid, (int, str)) for cid in campaign_ids):
                print("❌ Некоторые ID кампаний имеют неверный формат")
                print(f"Текущие ID: {campaign_ids}")
                return None
            
            # Конвертируем все ID в целые числа
            campaign_ids = [int(cid) for cid in campaign_ids]
            
            # Разбиваем список кампаний на группы по 3 кампании
            batch_size = 3
            campaign_batches = [campaign_ids[i:i + batch_size] for i in range(0, len(campaign_ids), batch_size)]
            
            print(f"\n📦 Разбиваем запрос на {len(campaign_batches)} частей")
            
            method = 'adgroups'
            field_names = [
                "Id",
                "Name",
                "CampaignId",
                "Status",
                "Type",
                "Subtype",
                "RegionIds"
            ]
            
            # Для хранения всех результатов
            all_adgroups = []
            total_units_used = 0
            total_units_remaining = None
            
            # Обрабатываем каждую группу кампаний
            for batch_num, campaign_batch in enumerate(campaign_batches, 1):
                print(f"\n🔄 Обработка части {batch_num}/{len(campaign_batches)}")
                print(f"📊 Кампании в текущей части: {campaign_batch}")
                
                params = {
                    "method": "get",
                    "params": {
                        "SelectionCriteria": {
                            "CampaignIds": campaign_batch
                        },
                        "FieldNames": field_names
                    }
                }
                
                response = requests.post(
                    f"{self.api_client.base_url}/{method}",
                    headers=self.api_client.headers,
                    json=params,
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'error' in result:
                        error = result['error']
                        print(f"❌ Ошибка API в части {batch_num}: {error.get('error_string', 'Неизвестная ошибка')}")
                        print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                        print(f"Детали ошибки: {error}")
                        continue
                    
                    # Добавляем информацию о баллах
                    if 'Units' in result:
                        units_info = result['Units']
                        used = units_info.get('Used', 0)
                        remaining = units_info.get('Remaining', 0)
                        total_units_used += used
                        total_units_remaining = remaining  # Берем последнее значение
                        print(f"📊 Потрачено баллов: {used}")
                        print(f"📊 Осталось баллов: {remaining}")
                    
                    # Собираем группы из текущего батча
                    if 'result' in result and 'AdGroups' in result['result']:
                        batch_adgroups = result['result']['AdGroups']
                        
                        # Фильтруем удаленные группы, если они есть
                        if deleted_group_ids:
                            filtered_adgroups = [
                                ag for ag in batch_adgroups 
                                if ag.get('Id') not in deleted_group_ids
                            ]
                            excluded_count = len(batch_adgroups) - len(filtered_adgroups)
                            if excluded_count > 0:
                                print(f"🚫 Исключено {excluded_count} групп из текущей части")
                            batch_adgroups = filtered_adgroups
                        
                        all_adgroups.extend(batch_adgroups)
                        print(f"✅ Получено групп в текущей части: {len(batch_adgroups)}")
                else:
                    print(f"❌ Ошибка HTTP в части {batch_num}: {response.status_code}")
                    print(f"Ответ: {response.text}")
                    continue
                
                # Небольшая пауза между запросами
                if batch_num < len(campaign_batches):
                    time.sleep(1)
            
            # Формируем итоговый результат
            if all_adgroups:
                final_result = {
                    "result": {
                        "AdGroups": all_adgroups
                    },
                    "Units": {
                        "Used": total_units_used,
                        "Remaining": total_units_remaining
                    },
                    "_meta": {
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "api_method": "adgroups.get",
                        "api_version": "v5",
                        "total_batches": len(campaign_batches),
                        "successful_batches": sum(1 for batch in all_adgroups if batch)
                    }
                }
                
                print(f"\n✅ Итого получено групп: {len(all_adgroups)}")
                if deleted_group_ids:
                    print(f"🚫 Исключено удаленных групп: {len(deleted_group_ids)}")
                print(f"📊 Всего потрачено баллов: {total_units_used}")
                
                return final_result
            else:
                print("❌ Не удалось получить данные ни в одной части")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения данных о группах: {e}")
            return None
    
    def save_adgroups_data(self, adgroups_data: Dict, report: Dict):
        """Сохраняет данные о группах в MinIO"""
        try:
            # Формируем путь для сохранения
            file_name = f"adgroups_{report['id']}.json"
            prefix = f"gen_report_context_contracts/data_yandex_direct/{report['id']}_результаты/"
            
            # Сохраняем в MinIO
            success = self.minio_client.upload_json_data(
                adgroups_data,
                file_name,
                report['id']
            )
            
            if success:
                print(f"💾 Данные о группах сохранены в MinIO для отчета {report['id']}")
            else:
                print(f"❌ Ошибка сохранения данных о группах в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения данных о группах: {e}")
    
    def display_adgroups_summary(self, adgroups_data: Dict):
        """Выводит краткую сводку по группам"""
        try:
            print(f"\n📊 Сводка по группам:")
            
            if not isinstance(adgroups_data, dict):
                print("   Ошибка: неверный формат данных")
                return
                
            result = adgroups_data.get('result', {})
            if not isinstance(result, dict):
                print("   Ошибка: неверный формат result")
                return
                
            adgroups = result.get('AdGroups', [])
            if not isinstance(adgroups, list):
                print("   Ошибка: неверный формат списка групп")
                return
            
            total_adgroups = len(adgroups)
            active_adgroups = sum(1 for ag in adgroups if ag.get('Status') == 'ACCEPTED')
            
            print(f"   Всего групп: {total_adgroups}")
            print(f"   Активных групп: {active_adgroups}")
            
            if adgroups:
                print(f"\n🔍 Первые 3 группы:")
                for i, adgroup in enumerate(adgroups[:3], 1):
                    name = adgroup.get('Name', 'N/A')
                    adgroup_id = adgroup.get('Id', 'N/A')
                    status = adgroup.get('Status', 'N/A')
                    campaign_id = adgroup.get('CampaignId', 'N/A')
                    
                    print(f"   {i}. {name}")
                    print(f"      ID: {adgroup_id}")
                    print(f"      Статус: {status}")
                    print(f"      ID кампании: {campaign_id}")
                    
        except Exception as e:
            print(f"   ❌ Ошибка при формировании сводки: {e}")
            print("   Проверьте формат данных в файле")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта получения данных о группах объявлений")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = AdGroupsDataProcessor()
    
    try:
        # Инициализируем MinIO клиент
        if not processor.minio_client.connect():
            print("❌ Не удалось подключиться к MinIO")
            return
        
        success = processor.process_reports()
        if success:
            print("\n✅ Обработка завершена успешно")
        else:
            print("\n❌ Обработка завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()
