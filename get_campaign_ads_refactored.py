#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Рефакторенная версия скрипта для получения объявлений по кампаниям из Яндекс.Директ
Использует общие модули database_manager и api_client
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

from database_manager import DatabaseManager
from api_client import DirectAPIClient

class CampaignAdsProcessor:
    """Обработчик данных кампаний и объявлений"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = None
        self.current_account = None
        self.results_dir = "Результаты"
    
    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск обработки отчетов")
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
            
            # Получаем удаленные группы для исключения
            deleted_group_ids = self.parse_deleted_groups(request_data)
            
            # Пытаемся получить объявления с разными аккаунтами
            ads_data = None
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
                    
                    # Получаем объявления
                    ads_data = self.api_client.get_ads_by_campaigns(campaign_ids)
                    if ads_data:
                        print("✅ Объявления получены успешно")
                        self.current_account = account
                        break
                    else:
                        print("❌ Не удалось получить объявления")
                else:
                    print("❌ Ошибка подключения к API")
                
                # Небольшая пауза между попытками
                time.sleep(2)
            
            if ads_data:
                # Фильтруем объявления по удаленным группам
                filtered_ads_data = self.filter_ads_by_deleted_groups(ads_data, deleted_group_ids)
                
                # Сохраняем данные
                self.save_ads_data(filtered_ads_data, report, request_data, contract_data)
            else:
                print("❌ Не удалось получить объявления ни с одним аккаунтом")
                
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
    
    def filter_ads_by_deleted_groups(self, ads_data: Dict, deleted_group_ids: List[int]) -> Dict:
        """Фильтрует объявления, исключая те, что принадлежат удаленным группам"""
        try:
            if not deleted_group_ids:
                print("ℹ️ Нет групп для исключения, возвращаем все объявления")
                return ads_data
            
            if 'result' not in ads_data or 'Ads' not in ads_data['result']:
                print("⚠️ Неверная структура данных объявлений")
                return ads_data
            
            original_ads = ads_data['result']['Ads']
            filtered_ads = []
            excluded_count = 0
            
            for ad in original_ads:
                ad_group_id = ad.get('AdGroupId')
                if ad_group_id in deleted_group_ids:
                    excluded_count += 1
                    print(f"🚫 Исключаем объявление ID {ad.get('Id')} из группы {ad_group_id}")
                else:
                    filtered_ads.append(ad)
            
            # Создаем отфильтрованный результат
            filtered_result = ads_data.copy()
            filtered_result['result'] = ads_data['result'].copy()
            filtered_result['result']['Ads'] = filtered_ads
            
            print(f"📊 Исходно объявлений: {len(original_ads)}")
            print(f"🚫 Исключено объявлений: {excluded_count}")
            print(f"✅ Осталось объявлений: {len(filtered_ads)}")
            
            return filtered_result
            
        except Exception as e:
            print(f"❌ Ошибка фильтрации объявлений: {e}")
            return ads_data
    
    def save_ads_data(self, ads_data: Dict, report: Dict, request_data: Dict, contract_data: Dict):
        """Сохраняет данные объявлений"""
        try:
            # Создаем имя файла
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ads_report_{report['id']}_{timestamp}.json"
            
            # Создаем папку для результатов
            os.makedirs(self.results_dir, exist_ok=True)
            
            filepath = os.path.join(self.results_dir, filename)
            
            # Сохраняем только чистые данные от Яндекса
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(ads_data, f, ensure_ascii=False, indent=2)
            
            print(f"💾 Данные сохранены в: {filepath}")
            
            # Выводим краткую статистику
            self.display_ads_summary(ads_data)
            
            # Извлекаем и выводим информацию о новых полях
            extracted_ads = self.extract_ads_data(ads_data)
            self.display_new_fields_info(extracted_ads)
            
        except Exception as e:
            print(f"❌ Ошибка сохранения данных: {e}")
    
    def extract_ads_data(self, ads_data: Dict) -> List[Dict]:
        """Извлекает данные объявлений из ответа API"""
        extracted_ads = []
        
        if 'result' not in ads_data:
            return extracted_ads
        
        ads = ads_data['result'].get('Ads', [])
        
        for ad in ads:
            ad_info = {
                'id': ad.get('Id'),
                'type': ad.get('Type'),
                'ad_group_id': ad.get('AdGroupId'),
                'campaign_id': ad.get('CampaignId'),
                'state': ad.get('State'),
                'status': ad.get('Status'),
                'title': '',
                'title2': '',
                'href': '',
                'display_url_path': '',
                'display_domain': '',
                'text': '',
                'sitelink_set_id': None,
                'ad_image_hash': '',
                'ad_extensions': []
            }
            
            # Извлекаем данные в зависимости от типа объявления
            ad_type = ad.get('Type', '')
            
            if ad_type == 'TEXT_AD' and 'TextAd' in ad:
                text_ad = ad['TextAd']
                ad_info.update({
                    'title': text_ad.get('Title', ''),
                    'title2': text_ad.get('Title2', ''),
                    'href': text_ad.get('Href', ''),
                    'display_url_path': text_ad.get('DisplayUrlPath', ''),
                    'display_domain': text_ad.get('DisplayDomain', ''),
                    'text': text_ad.get('Text', ''),
                    'sitelink_set_id': text_ad.get('SitelinkSetId'),
                    'ad_image_hash': text_ad.get('AdImageHash', '')
                })
                
                # Обрабатываем расширения
                if 'AdExtensions' in text_ad:
                    for ext in text_ad['AdExtensions']:
                        ad_info['ad_extensions'].append({
                            'id': ext.get('AdExtensionId'),
                            'type': ext.get('Type')
                        })
            
            elif ad_type == 'MOBILE_APP_AD' and 'MobileAppAd' in ad:
                mobile_app_ad = ad['MobileAppAd']
                ad_info.update({
                    'title': mobile_app_ad.get('Title', ''),
                    'text': mobile_app_ad.get('Text', ''),
                    'ad_image_hash': mobile_app_ad.get('AdImageHash', '')
                })
            
            elif ad_type == 'DYNAMIC_TEXT_AD' and 'DynamicTextAd' in ad:
                dynamic_text_ad = ad['DynamicTextAd']
                ad_info.update({
                    'text': dynamic_text_ad.get('Text', ''),
                    'ad_image_hash': dynamic_text_ad.get('AdImageHash', '')
                })
                
                # Обрабатываем расширения
                if 'AdExtensions' in dynamic_text_ad:
                    for ext in dynamic_text_ad['AdExtensions']:
                        ad_info['ad_extensions'].append({
                            'id': ext.get('AdExtensionId'),
                            'type': ext.get('Type')
                        })
            
            elif ad_type == 'TEXT_IMAGE_AD' and 'TextImageAd' in ad:
                text_image_ad = ad['TextImageAd']
                ad_info.update({
                    'title': text_image_ad.get('Title', ''),
                    'href': text_image_ad.get('Href', ''),
                    'text': text_image_ad.get('Text', ''),
                    'ad_image_hash': text_image_ad.get('AdImageHash', '')
                })
            
            elif ad_type == 'MOBILE_APP_IMAGE_AD' and 'MobileAppImageAd' in ad:
                mobile_app_image_ad = ad['MobileAppImageAd']
                ad_info.update({
                    'ad_image_hash': mobile_app_image_ad.get('AdImageHash', '')
                })
            
            extracted_ads.append(ad_info)
        
        return extracted_ads

    def display_ads_summary(self, ads_data: Dict):
        """Выводит краткую сводку по объявлениям"""
        try:
            if 'result' not in ads_data:
                print("⚠️ Нет данных в результате")
                return
            
            ads = ads_data['result'].get('Ads', [])
            print(f"\n📊 Найдено объявлений: {len(ads)}")
            
            if ads:
                print("📋 Первые 5 объявлений:")
                for i, ad in enumerate(ads[:5], 1):
                    ad_type = ad.get('Type', 'N/A')
                    campaign_id = ad.get('CampaignId', 'N/A')
                    state = ad.get('State', 'N/A')
                    print(f"   {i}. Тип: {ad_type}, Кампания: {campaign_id}, Состояние: {state}")
                
                if len(ads) > 5:
                    print(f"   ... и еще {len(ads) - 5} объявлений")
            
        except Exception as e:
            print(f"❌ Ошибка вывода сводки: {e}")
    
    def display_new_fields_info(self, extracted_ads: List[Dict]):
        """Выводит информацию о новых полях объявлений"""
        try:
            ads_with_title2 = [ad for ad in extracted_ads if ad.get('title2')]
            ads_with_display_url = [ad for ad in extracted_ads if ad.get('display_url_path')]
            ads_with_display_domain = [ad for ad in extracted_ads if ad.get('display_domain')]
            
            print(f"\n📋 Информация о новых полях:")
            print(f"   📝 Объявлений с дополнительным заголовком (Title2): {len(ads_with_title2)}")
            print(f"   🔗 Объявлений с отображаемой ссылкой (DisplayUrlPath): {len(ads_with_display_url)}")
            print(f"   🌐 Объявлений с доменом отображаемой ссылки (DisplayDomain): {len(ads_with_display_domain)}")
            
            # Показываем примеры новых полей
            if ads_with_title2 or ads_with_display_url or ads_with_display_domain:
                print(f"\n📄 Примеры новых полей:")
                for i, ad in enumerate(extracted_ads[:3], 1):  # Показываем первые 3 объявления
                    if ad.get('title2') or ad.get('display_url_path') or ad.get('display_domain'):
                        print(f"   {i}. ID: {ad.get('id')}")
                        if ad.get('title2'):
                            print(f"      📝 Дополнительный заголовок: {ad.get('title2')}")
                        if ad.get('display_url_path'):
                            print(f"      🔗 Отображаемая ссылка: {ad.get('display_url_path')}")
                        if ad.get('display_domain'):
                            print(f"      🌐 Домен отображаемой ссылки: {ad.get('display_domain')}")
                        print()
            
        except Exception as e:
            print(f"❌ Ошибка вывода информации о новых полях: {e}")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта получения объявлений по кампаниям")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл config.env")
        return
    
    # Создаем и запускаем обработчик
    processor = CampaignAdsProcessor()
    
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
