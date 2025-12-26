#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Рефакторенная версия скрипта для извлечения уникальных хешей изображений из ads_report файла
Использует общие модули database_manager и api_client
"""

import os
import json
import time
from datetime import datetime
from typing import Dict, List, Set, Optional

from database_manager import DatabaseManager
from api_client import DirectAPIClient
from minio_client import MinIOClient

class ImageHashesProcessor:
    """Обработчик для извлечения хешей изображений и получения ссылок"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.minio_client = MinIOClient()
        self.results_dir = "Результаты"
        self.api_client = None
        self.current_account = None
    
    def process_image_hashes(self):
        """Основной метод обработки хешей изображений"""
        print("🖼️ Извлечение хешей изображений из ads_report")
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
            
            # Извлекаем уникальные хеши
            unique_hashes = self.extract_unique_image_hashes(ads_data)
            if not unique_hashes:
                print("❌ Уникальных хешей изображений не найдено")
                return True
            
            # Получаем URL изображений
            images_data = self.get_image_urls(unique_hashes)
            if images_data:
                self.save_image_data(images_data, report['id'])
            
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
    
    def extract_unique_image_hashes(self, ads_data: Dict) -> Set[str]:
        """Извлекает уникальные хеши изображений"""
        unique_hashes = set()
        
        try:
            # Поддерживаем оба формата: новый (ads) и старый (result.Ads)
            if 'ads' in ads_data:
                ads = ads_data['ads']
            elif 'result' in ads_data and 'Ads' in ads_data['result']:
                ads = ads_data['result']['Ads']
            else:
                return unique_hashes
            
            print(f"📊 Обрабатываем {len(ads)} объявлений для извлечения хешей изображений")
            
            for ad in ads:
                ad_type = ad.get('Type', '')
                
                # Проверяем разные типы объявлений
                if ad_type == 'TEXT_AD' and 'TextAd' in ad:
                    hash_value = ad['TextAd'].get('AdImageHash')
                    if hash_value and hash_value not in unique_hashes:
                        unique_hashes.add(hash_value)
                        print(f"✅ Найден AdImageHash: {hash_value}")
                
                elif ad_type == 'MOBILE_APP_AD' and 'MobileAppAd' in ad:
                    hash_value = ad['MobileAppAd'].get('AdImageHash')
                    if hash_value and hash_value not in unique_hashes:
                        unique_hashes.add(hash_value)
                        print(f"✅ Найден AdImageHash (MobileAppAd): {hash_value}")
                
                elif ad_type == 'DYNAMIC_TEXT_AD' and 'DynamicTextAd' in ad:
                    hash_value = ad['DynamicTextAd'].get('AdImageHash')
                    if hash_value and hash_value not in unique_hashes:
                        unique_hashes.add(hash_value)
                        print(f"✅ Найден AdImageHash (DynamicTextAd): {hash_value}")
                
                elif ad_type == 'TEXT_IMAGE_AD' and 'TextImageAd' in ad:
                    hash_value = ad['TextImageAd'].get('AdImageHash')
                    if hash_value and hash_value not in unique_hashes:
                        unique_hashes.add(hash_value)
                        print(f"✅ Найден AdImageHash (TextImageAd): {hash_value}")
                
                elif ad_type == 'MOBILE_APP_IMAGE_AD' and 'MobileAppImageAd' in ad:
                    hash_value = ad['MobileAppImageAd'].get('AdImageHash')
                    if hash_value and hash_value not in unique_hashes:
                        unique_hashes.add(hash_value)
                        print(f"✅ Найден AdImageHash (MobileAppImageAd): {hash_value}")
            
            print(f"\n📊 Итого найдено уникальных хешей изображений: {len(unique_hashes)}")
            
            return unique_hashes
            
        except Exception as e:
            print(f"❌ Ошибка извлечения хешей: {e}")
            return unique_hashes
    
    def get_image_urls(self, unique_hashes: Set[str]) -> Optional[Dict]:
        """Получает URL изображений по хешам"""
        if not unique_hashes:
            print("⚠️ Список хешей изображений пуст")
            return None
        
        print(f"\n🖼️ Получение URL для {len(unique_hashes)} изображений")
        
        # Получаем данные изображений через API
        print(f"🖼️ Запрос данных изображений через API...")
        image_data = self.api_client.get_image_urls_by_hashes(list(unique_hashes))
        
        if image_data:
            print("✅ Данные изображений получены успешно")
            return image_data
        else:
            print("❌ Не удалось получить данные изображений")
            return None
    
    def save_image_data(self, image_data: Dict, report_id: int):
        """Сохраняет данные изображений в MinIO"""
        try:
            # Сохраняем в MinIO
            success = self.minio_client.upload_json_data(
                image_data,
                f"image_hashes_{report_id}.json",
                report_id
            )
            
            if success:
                print(f"💾 Данные изображений сохранены в MinIO для отчета {report_id}")
                # Выводим сводку
                self.display_image_summary(image_data)
            else:
                print("❌ Ошибка сохранения данных изображений в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения данных изображений: {e}")
    
    def display_image_summary(self, image_data: Dict):
        """Выводит краткую сводку по изображениям"""
        print(f"\n📊 Сводка по изображениям:")
        
        if 'result' in image_data and 'AdImages' in image_data['result']:
            images = image_data['result']['AdImages']
            print(f"   Найдено изображений: {len(images)}")
            
            if images:
                print(f"\n🔍 Первые 3 изображения:")
                for i, image in enumerate(images[:3]):
                    hash_value = image.get('AdImageHash', 'N/A')
                    original_url = image.get('OriginalUrl', 'N/A')
                    preview_url = image.get('PreviewUrl', 'N/A')
                    
                    print(f"   {i+1}. Хеш: {hash_value}")
                    print(f"      Original URL: {original_url}")
                    print(f"      Preview URL: {preview_url}")
        else:
            print("   Изображения не найдены")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта извлечения хешей изображений")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = ImageHashesProcessor()
    
    try:
        success = processor.process_image_hashes()
        if success:
            print("\n✅ Обработка завершена успешно")
        else:
            print("\n❌ Обработка завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()