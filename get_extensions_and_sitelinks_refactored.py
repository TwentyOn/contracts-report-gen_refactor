#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Рефакторенная версия скрипта для извлечения и скачивания SitelinkSetId и AdExtensions
Использует общие модули database_manager и api_client
"""

import os
import json
import glob
import time
from datetime import datetime
from typing import Dict, List, Set, Optional, Any

from database_manager import DatabaseManager
from api_client import DirectAPIClient
from minio_client import MinIOClient

class ExtensionsProcessor:
    """Обработчик для извлечения и скачивания расширений"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.minio_client = MinIOClient()
        self.results_dir = "Результаты"
        self.api_client = None
        self.current_account = None
    
    def process_extensions(self):
        """Основной метод обработки расширений"""
        print("🚀 Запуск обработки расширений и быстрых ссылок")
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
            
            # Обрабатываем каждый отчет
            for report in reports:
                print(f"\n📋 Обработка отчета ID: {report['id']}")
                success = self.process_single_report(report)
                if success:
                    return True
            
            print("❌ Не найдены отчеты с файлами объявлений")
            return False
            
        finally:
            self.db.disconnect()
    
    def process_single_report(self, report: Dict) -> bool:
        """Обрабатывает один отчет"""
        try:
            # Получаем данные заявки
            request_data = self.db.get_request_data(report['id_requests'])
            if not request_data:
                print(f"❌ Не найдены данные заявки ID: {report['id_requests']}")
                return False
            
            # Получаем данные договора
            contract_data = self.db.get_contract_data(report['id_contracts'])
            if not contract_data:
                print(f"❌ Не найдены данные договора ID: {report['id_contracts']}")
                return False
            
            # Получаем аккаунты из БД
            accounts = self.db.get_yandex_accounts()
            if not accounts:
                print("❌ Не найдены аккаунты Яндекс.Директ")
                return False
            
            # Настраиваем API клиент
            if not self.setup_api_client(accounts, contract_data):
                print("❌ Не удалось настроить API клиент")
                return False
            
            # Загружаем данные объявлений из MinIO
            ads_data = self.load_ads_report_from_minio(report['id'])
            if not ads_data:
                print("❌ Не удалось загрузить данные объявлений из MinIO")
                return False
            
            # Извлекаем уникальные ID
            unique_ids = self.extract_unique_ids(ads_data)
            sitelink_set_ids = unique_ids['sitelink_set_ids']
            extension_ids = unique_ids['extension_ids']
            
            if not sitelink_set_ids and not extension_ids:
                print("⚠️ Не найдены SitelinkSetId и AdExtensionId для скачивания")
                return True
            
            # Скачиваем быстрые ссылки
            sitelinks_data = {}
            if sitelink_set_ids:
                sitelinks_data = self.download_sitelinks(sitelink_set_ids)
                if sitelinks_data:
                    self.save_sitelinks_data(sitelinks_data, report['id'])
            
            # Скачиваем расширения
            extensions_data = {}
            if extension_ids:
                extensions_data = self.download_extensions(extension_ids)
                if extensions_data:
                    self.save_extensions_data(extensions_data, report['id'])
            
            print("✅ Обработка отчета завершена успешно")
            return True
                
        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")
            return False
    
    def setup_api_client(self, accounts: List[Dict], contract_data: Dict) -> bool:
        """Настраивает API клиент с правильным аккаунтом"""
        try:
            # Используем первый доступный аккаунт
            account = accounts[0]
            print(f"✅ Используем аккаунт: {account['comment']}")
            
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
            
            self.current_account = account
            return True
            
        except Exception as e:
            print(f"❌ Ошибка настройки API клиента: {e}")
            return False
    
    
    def load_ads_report_from_minio(self, report_id: int) -> Optional[Dict]:
        """Загружает файл с объявлениями из MinIO"""
        try:
            # Формируем путь к файлу
            file_path = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/ads_report_{report_id}.json"
            
            # Получаем данные из MinIO
            response = self.minio_client.client.get_object(
                self.minio_client.bucket_name,
                file_path
            )
            
            # Читаем и парсим JSON
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            
            print(f"✅ Файл загружен из MinIO для отчета: {report_id}")
            return data
            
        except Exception as e:
            print(f"❌ Ошибка загрузки файла из MinIO: {e}")
            return None
    
    def extract_unique_ids(self, ads_data: Dict) -> Dict[str, Set[int]]:
        """Извлекает уникальные ID из данных объявлений"""
        sitelink_set_ids = set()
        extension_ids = set()
        sitelink_count = 0
        extension_count = 0
        
        try:
            # Поддерживаем оба формата: новый (ads) и старый (result.Ads)
            if 'ads' in ads_data:
                ads = ads_data['ads']
            elif 'result' in ads_data and 'Ads' in ads_data['result']:
                ads = ads_data['result']['Ads']
            else:
                print("❌ Не найдены данные объявлений в файле")
                return {'sitelink_set_ids': sitelink_set_ids, 'extension_ids': extension_ids}
            
            print(f"📊 Обрабатываем {len(ads)} объявлений")
            
            for ad in ads:
                # Извлекаем SitelinkSetId из TextAd
                if 'TextAd' in ad and 'SitelinkSetId' in ad['TextAd']:
                    sitelink_id = ad['TextAd']['SitelinkSetId']
                    if sitelink_id:
                        sitelink_set_ids.add(sitelink_id)
                        if sitelink_count < 10:
                            print(f"🔗 Найден SitelinkSetId: {sitelink_id}")
                            sitelink_count += 1
                
                # Извлекаем AdExtensions из TextAd
                if 'TextAd' in ad and 'AdExtensions' in ad['TextAd']:
                    extensions = ad['TextAd']['AdExtensions']
                    for ext in extensions:
                        if 'AdExtensionId' in ext:
                            ext_id = ext['AdExtensionId']
                            if ext_id:
                                extension_ids.add(ext_id)
                                if extension_count < 10:
                                    print(f"🔧 Найден AdExtensionId: {ext_id}")
                                    extension_count += 1
                
                # Извлекаем AdExtensions из DynamicTextAd
                if 'DynamicTextAd' in ad and 'AdExtensions' in ad['DynamicTextAd']:
                    extensions = ad['DynamicTextAd']['AdExtensions']
                    for ext in extensions:
                        if 'AdExtensionId' in ext:
                            ext_id = ext['AdExtensionId']
                            if ext_id:
                                extension_ids.add(ext_id)
                                if extension_count < 10:
                                    print(f"🔧 Найден AdExtensionId (DynamicTextAd): {ext_id}")
                                    extension_count += 1
            
            print(f"📊 Найдено уникальных SitelinkSetId: {len(sitelink_set_ids)}")
            print(f"📊 Найдено уникальных AdExtensionId: {len(extension_ids)}")
            
            return {
                'sitelink_set_ids': sitelink_set_ids,
                'extension_ids': extension_ids
            }
            
        except Exception as e:
            print(f"❌ Ошибка извлечения ID: {e}")
            return {'sitelink_set_ids': sitelink_set_ids, 'extension_ids': extension_ids}
    
    def download_sitelinks(self, sitelink_set_ids: Set[int]) -> Dict:
        """Скачивает быстрые ссылки"""
        print(f"\n🔗 Скачивание быстрых ссылок для {len(sitelink_set_ids)} наборов")
        
        all_sitelinks_data = {}
        
        for sitelink_id in sitelink_set_ids:
            print(f"\n📥 Скачивание SitelinkSetId: {sitelink_id}")
            
            sitelink_data = self.api_client.get_sitelinks_by_set_id(sitelink_id)
            if sitelink_data:
                all_sitelinks_data[sitelink_id] = sitelink_data
                print(f"✅ SitelinkSetId {sitelink_id} скачан успешно")
            else:
                print(f"❌ Не удалось скачать SitelinkSetId {sitelink_id}")
        
        return all_sitelinks_data
    
    def download_extensions(self, extension_ids: Set[int]) -> Dict:
        """Скачивает расширения"""
        print(f"\n🔧 Скачивание расширений для {len(extension_ids)} ID")
        
        # Разбиваем на батчи по 1000 ID (лимит API)
        extension_ids_list = list(extension_ids)
        batch_size = 1000
        all_extensions_data = {}
        
        for i in range(0, len(extension_ids_list), batch_size):
            batch = extension_ids_list[i:i + batch_size]
            print(f"\n📥 Скачивание батча расширений: {len(batch)} ID")
            
            extensions_data = self.api_client.get_extensions_by_ids(batch)
            if extensions_data:
                all_extensions_data[f'batch_{i//batch_size + 1}'] = extensions_data
                print(f"✅ Батч {i//batch_size + 1} скачан успешно")
            else:
                print(f"❌ Не удалось скачать батч {i//batch_size + 1}")
        
        return all_extensions_data
    
    def save_sitelinks_data(self, sitelinks_data: Dict, report_id: int):
        """Сохраняет данные быстрых ссылок в MinIO"""
        try:
            # Сохраняем в MinIO
            success = self.minio_client.upload_json_data(
                sitelinks_data,
                f"sitelinks_{report_id}.json",
                report_id
            )
            
            if success:
                print(f"💾 Данные быстрых ссылок сохранены в MinIO для отчета {report_id}")
            else:
                print("❌ Ошибка сохранения данных быстрых ссылок в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения быстрых ссылок: {e}")
    
    def save_extensions_data(self, extensions_data: Dict, report_id: int):
        """Сохраняет данные расширений в MinIO"""
        try:
            # Сохраняем в MinIO
            success = self.minio_client.upload_json_data(
                extensions_data,
                f"extensions_{report_id}.json",
                report_id
            )
            
            if success:
                print(f"💾 Данные расширений сохранены в MinIO для отчета {report_id}")
            else:
                print("❌ Ошибка сохранения данных расширений в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения расширений: {e}")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта скачивания расширений и быстрых ссылок")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = ExtensionsProcessor()
    
    try:
        success = processor.process_extensions()
        if success:
            print("\n✅ Обработка завершена успешно")
        else:
            print("\n❌ Обработка завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()
