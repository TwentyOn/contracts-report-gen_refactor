#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для получения данных о кампаниях, включая минус-слова
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

class CampaignsDataProcessor:
    """Обработчик для получения данных о кампаниях"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = None
        self.current_account = None
    
    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск получения данных о кампаниях")
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
            
            # Пытаемся получить данные с разными аккаунтами
            campaigns_data = None
            
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
                    
                    # Получаем данные о кампаниях
                    campaigns_data = self.get_campaigns_data()
                    if campaigns_data:
                        print("✅ Данные о кампаниях получены успешно")
                        self.current_account = account
                        break
                    else:
                        print("❌ Не удалось получить данные о кампаниях")
                else:
                    print("❌ Ошибка подключения к API")
                
                # Небольшая пауза между попытками
                time.sleep(2)
            
            if campaigns_data:
                # Сохраняем данные
                self.save_campaigns_data(campaigns_data, report)
            else:
                print("❌ Не удалось получить данные ни с одним аккаунтом")
                
        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")
    
    def get_campaigns_data(self) -> Optional[Dict]:
        """Получает данные о кампаниях через API"""
        try:
            method = 'campaigns'
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {},
                    "FieldNames": [
                        "Id",
                        "Name",
                        "Type",
                        "Status",
                        "State",
                        "StatusPayment",
                        "StatusClarification",
                        "Currency",
                        "Funds",
                        "RepresentedBy",
                        "NegativeKeywords"  # Добавляем поле для получения минус-слов
                    ],
                    "TextCampaignFieldNames": [
                        "CounterIds",
                        "RelevantKeywords",
                        "Settings",
                        "BiddingStrategy",
                        "PriorityGoals",
                        "AttributionModel",
                        "PackageBiddingStrategy",
                        "CanBeUsedAsPackageBiddingStrategySource",
                        "NegativeKeywordSharedSetIds"
                    ],
                    "UnifiedCampaignFieldNames": [
                        "CounterIds",
                        "Settings",
                        "BiddingStrategy",
                        "PriorityGoals",
                        "TrackingParams",
                        "AttributionModel",
                        "PackageBiddingStrategy",
                        "CanBeUsedAsPackageBiddingStrategySource"
                    ],
                    "UnifiedCampaignPackageBiddingStrategyPlatformsFieldNames": [
                        "SearchResult",
                        "ProductGallery",
                        "Maps",
                        "SearchOrganizationList",
                        "Network",
                        "DynamicPlaces"
                    ]
                }
            }
            
            print("🔍 Запрос данных о кампаниях...")
            
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
                    print(f"❌ Ошибка API: {error.get('error_string', 'Неизвестная ошибка')}")
                    print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                    return None
                
                # Добавляем информацию о баллах
                if 'Units' in result:
                    units_info = result['Units']
                    print(f"📊 Потрачено баллов: {units_info.get('Used', 'N/A')}")
                    print(f"📊 Осталось баллов: {units_info.get('Remaining', 'N/A')}")
                
                # Добавляем метаинформацию
                result['_meta'] = {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'api_method': 'campaigns.get',
                    'api_version': 'v5'
                }
                
                # Выводим статистику
                self.display_campaigns_summary(result)
                
                return result
            else:
                print(f"❌ Ошибка HTTP: {response.status_code}")
                print(f"Ответ: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения данных о кампаниях: {e}")
            return None
    
    def save_campaigns_data(self, campaigns_data: Dict):
        """Выводит сводку по данным о кампаниях"""
        try:
            # Выводим сводку
            self.display_campaigns_summary(campaigns_data)
        except Exception as e:
            print(f"❌ Ошибка отображения сводки: {e}")
    
    def display_campaigns_summary(self, campaigns_data: Dict):
        """Выводит краткую сводку по кампаниям"""
        try:
            print(f"\n📊 Сводка по кампаниям:")
            
            if not isinstance(campaigns_data, dict):
                print("   Ошибка: неверный формат данных")
                return
                
            result = campaigns_data.get('result', {})
            if not isinstance(result, dict):
                print("   Ошибка: неверный формат result")
                return
                
            campaigns = result.get('Campaigns', [])
            if not isinstance(campaigns, list):
                print("   Ошибка: неверный формат списка кампаний")
                return
            
            total_campaigns = len(campaigns)
            campaigns_with_minus = 0
            total_minus_words = 0
            
            # Подсчитываем статистику
            for campaign in campaigns:
                if not isinstance(campaign, dict):
                    continue
                    
                negative_keywords = campaign.get('NegativeKeywords', {})
                if isinstance(negative_keywords, dict):
                    items = negative_keywords.get('Items', [])
                    if items:
                        campaigns_with_minus += 1
                        total_minus_words += len(items)
            
            print(f"   Найдено кампаний: {total_campaigns}")
            print(f"   Кампаний с минус-словами: {campaigns_with_minus}")
            print(f"   Всего минус-слов: {total_minus_words}")
            
            if campaigns:
                print(f"\n🔍 Первые 3 кампании:")
                shown_campaigns = 0
                for campaign in campaigns:
                    if shown_campaigns >= 3:
                        break
                        
                    if not isinstance(campaign, dict):
                        continue
                    
                    name = campaign.get('Name', 'N/A')
                    campaign_id = campaign.get('Id', 'N/A')
                    status = campaign.get('Status', 'N/A')
                    
                    print(f"   {shown_campaigns + 1}. {name} (ID: {campaign_id}, Статус: {status})")
                    
                    negative_keywords = campaign.get('NegativeKeywords', {})
                    if isinstance(negative_keywords, dict):
                        items = negative_keywords.get('Items', [])
                        if items:
                            print(f"      Минус-слова: {', '.join(items)}")
                    
                    shown_campaigns += 1
                    
        except Exception as e:
            print(f"   ❌ Ошибка при формировании сводки: {e}")
            print("   Проверьте формат данных в файле")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта получения данных о кампаниях")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = CampaignsDataProcessor()
    
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
