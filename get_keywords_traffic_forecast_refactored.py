#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Рефакторенная версия скрипта для получения прогнозных данных по ключевым фразам
Использует общие модули database_manager и api_client
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from database_manager import DatabaseManager
from api_client import DirectAPIClient
from minio_client import MinIOClient

class KeywordsTrafficProcessor:
    """Обработчик данных ключевых фраз и прогнозов трафика"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = None
        self.current_account = None
        self.minio_client = MinIOClient()
        self.results_dir = "Результаты"
    
    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск обработки прогнозов трафика")
        print("="*60)
        
        # Подключаемся к БД
        if not self.db.connect():
            return False
            
        # Подключаемся к MinIO
        if not self.minio_client.connect():
            print("❌ Не удалось подключиться к MinIO")
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
            # Получаем данные заявки
            request_data = self.db.get_request_data(report['id_requests'])
            if not request_data:
                print(f"❌ Не найдены данные заявки ID: {report['id_requests']}")
                return
            
            # Получаем данные договора
            contract_data = self.db.get_contract_data(report['id_contracts'])
            if not contract_data:
                print(f"❌ Не найдены данные договора ID: {report['id_contracts']}")
                return
            
            # Извлекаем ID кампаний из JSON
            campaign_data = request_data.get('campany_yandex_direct')
            if not campaign_data:
                print("❌ Не найдены данные кампаний в заявке")
                return
            
            campaign_ids = self.db.extract_campaign_ids(campaign_data)
            if not campaign_ids:
                print("❌ Не найдены ID кампаний")
                return
            
            print(f"📊 Найдено кампаний: {len(campaign_ids)}")
            print(f"📊 ID кампаний: {campaign_ids}")
            
            # Пытаемся получить данные с разными аккаунтами
            keywords_data = None
            
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
                    
                    # Загружаем группы из MinIO
                    adgroup_ids = self.load_adgroups_from_minio(report['id'])
                    if adgroup_ids:
                        # Получаем ключевые фразы по группам
                        keywords_data = self.api_client.get_keywords_by_adgroups(adgroup_ids)
                        if keywords_data:
                            print("✅ Ключевые фразы получены успешно")
                            self.current_account = account
                            break
                        else:
                            print("❌ Не удалось получить ключевые фразы")
                    else:
                        print("❌ Не удалось загрузить группы из MinIO")
                else:
                    print("❌ Ошибка подключения к API")
                
                # Небольшая пауза между попытками
                time.sleep(2)
            
            if keywords_data:
                # Сохраняем данные
                self.save_keywords_data(keywords_data, report)
            else:
                print("❌ Не удалось получить данные ни с одним аккаунтом")
                
        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")
    
    def save_keywords_data(self, keywords_data: Dict, report: Dict):
        """Сохраняет данные ключевых фраз в MinIO"""
        try:
            # Сохраняем в MinIO
            success = self.minio_client.upload_json_data(
                keywords_data,
                f"keywords_traffic_forecast_{report['id']}.json",
                report['id']
            )
            
            if success:
                print(f"💾 Данные ключевых фраз сохранены в MinIO для отчета {report['id']}")
                # Выводим сводку
                self.display_keywords_summary(keywords_data)
            else:
                print("❌ Ошибка сохранения данных в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения данных: {e}")
    
    def load_adgroups_from_minio(self, report_id: int) -> Optional[List[int]]:
        """Загружает ID групп из сохраненного файла в MinIO"""
        try:
            # Формируем путь к файлу
            file_path = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/adgroups_{report_id}.json"
            
            # Получаем данные из MinIO
            response = self.minio_client.client.get_object(
                self.minio_client.bucket_name,
                file_path
            )
            
            # Читаем и парсим JSON
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            
            # Извлекаем ID групп
            if 'result' in data and 'AdGroups' in data['result']:
                adgroups = data['result']['AdGroups']
                adgroup_ids = [ag['Id'] for ag in adgroups if ag.get('Status') == 'ACCEPTED']
                print(f"✅ Загружено групп из MinIO: {len(adgroup_ids)}")
                return adgroup_ids
            else:
                print("❌ Неверный формат данных групп в MinIO")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка загрузки групп из MinIO: {e}")
            return None
    
    def display_keywords_summary(self, keywords_data: Dict):
        """Выводит краткую сводку по ключевым фразам"""
        print(f"\n📊 Сводка по ключевым фразам:")
        
        if 'result' in keywords_data and 'Keywords' in keywords_data['result']:
            keywords = keywords_data['result']['Keywords']
            print(f"   Найдено ключевых фраз: {len(keywords)}")
            
            if keywords:
                print(f"\n🔍 Первые 3 ключевые фразы:")
                for i, keyword in enumerate(keywords[:3]):
                    keyword_text = keyword.get('Keyword', 'N/A')
                    keyword_id = keyword.get('Id', 'N/A')
                    minus_keywords = keyword.get('MinusKeywords', [])
                    
                    print(f"   {i+1}. {keyword_text} (ID: {keyword_id})")
                    if minus_keywords:
                        print(f"      Минус-слова: {', '.join(minus_keywords)}")
        else:
            print("   Ключевые фразы не найдены")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта получения ключевых фраз")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = KeywordsTrafficProcessor()
    
    try:
        success = processor.process_reports()
        if success:
            print("\n✅ Обработка завершена успешно")
        else:
            print("\n❌ Обработка завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()