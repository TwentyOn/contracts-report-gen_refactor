#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Модуль для работы с MinIO
Содержит класс для загрузки файлов в MinIO bucket
"""

import os
import json
import io
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# Загружаем переменные окружения
load_dotenv('.env')

class MinIOClient:
    """Клиент для работы с MinIO"""

    def __init__(self):
        self.client = None
        self.bucket_name = None
        self.base_path = "gen_report_context_contracts/data_yandex_direct"

        # Получаем настройки из переменных окружения
        self.endpoint = os.getenv('S3_ENDPOINT_URL')
        self.access_key = os.getenv('S3_ACCESS_KEY')
        self.secret_key = os.getenv('S3_SECRET_KEY')
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        self.secure = os.getenv('S3_SECURE', 'False').lower() == 'true'

        if not all([self.endpoint, self.access_key, self.secret_key, self.bucket_name]):
            raise ValueError("Не все переменные окружения для MinIO настроены")

    def connect(self) -> bool:
        """Подключается к MinIO"""
        try:
            # Импортируем urllib3 для настройки таймаутов
            import urllib3

            # Настраиваем пул соединений с увеличенными таймаутами
            http_client = urllib3.PoolManager(
                timeout=urllib3.Timeout(connect=30, read=300),  # 30 сек на подключение, 5 мин на чтение
                retries=urllib3.Retry(
                    total=3,
                    backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504]
                )
            )

            self.client = Minio(
                self.endpoint,
                access_key=self.access_key,
                secret_key=self.secret_key,
                secure=self.secure,
                http_client=http_client
            )

            # Проверяем существование bucket
            if not self.client.bucket_exists(self.bucket_name):
                print(f"❌ Bucket '{self.bucket_name}' не существует")
                return False

            print(f"✅ Подключение к MinIO успешно")
            print(f"📦 Bucket: {self.bucket_name}")
            print(f"🔗 Endpoint: {self.endpoint}")
            return True

        except Exception as e:
            print(f"❌ Ошибка подключения к MinIO: {e}")
            return False

    def upload_json_data(self, data: Dict, filename: str, report_id: int) -> bool:
        """Загружает JSON данные в MinIO"""
        try:
            if not self.client:
                print("❌ MinIO клиент не инициализирован")
                return False

            # Создаем путь к файлу - используем фиксированную папку для отчета
            object_name = f"{self.base_path}/{report_id}_результаты/{filename}"

            # Конвертируем данные в JSON строку
            json_str = json.dumps(data, ensure_ascii=False, indent=2)
            json_bytes = json_str.encode('utf-8')

            # Создаем поток данных
            data_stream = io.BytesIO(json_bytes)

            # Загружаем в MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=data_stream,
                length=len(json_bytes),
                content_type='application/json'
            )

            print(f"💾 Данные сохранены в MinIO: {object_name}")
            return True

        except S3Error as e:
            print(f"❌ Ошибка S3 при загрузке файла: {e}")
            return False
        except Exception as e:
            print(f"❌ Ошибка загрузки файла в MinIO: {e}")
            return False

    def upload_tsv_data(self, tsv_content: str, filename: str, report_id: int) -> bool:
        """Загружает TSV данные в MinIO"""
        try:
            if not self.client:
                print("❌ MinIO клиент не инициализирован")
                return False

            # Создаем путь к файлу - используем фиксированную папку для отчета
            object_name = f"{self.base_path}/{report_id}_результаты/{filename}"

            # Конвертируем TSV в байты
            tsv_bytes = tsv_content.encode('utf-8')

            # Создаем поток данных
            data_stream = io.BytesIO(tsv_bytes)

            # Загружаем в MinIO
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=data_stream,
                length=len(tsv_bytes),
                content_type='text/tab-separated-values'
            )

            print(f"💾 TSV данные сохранены в MinIO: {object_name}")
            return True

        except S3Error as e:
            print(f"❌ Ошибка S3 при загрузке TSV файла: {e}")
            return False
        except Exception as e:
            print(f"❌ Ошибка загрузки TSV файла в MinIO: {e}")
            return False

    def upload_ads_data(self, ads_data: Dict, report_id: int) -> bool:
        """Загружает данные объявлений"""
        try:
            filename = f"ads_report_{report_id}.json"
            return self.upload_json_data(ads_data, filename, report_id)
        except Exception as e:
            print(f"❌ Ошибка загрузки данных объявлений: {e}")
            return False

    def upload_sitelinks_data(self, sitelinks_data: Dict, report_id: int) -> bool:
        """Загружает данные быстрых ссылок"""
        try:
            filename = f"sitelinks_{report_id}.json"
            return self.upload_json_data(sitelinks_data, filename, report_id)
        except Exception as e:
            print(f"❌ Ошибка загрузки данных быстрых ссылок: {e}")
            return False

    def upload_extensions_data(self, extensions_data: Dict, report_id: int) -> bool:
        """Загружает данные расширений"""
        try:
            filename = f"extensions_{report_id}.json"
            return self.upload_json_data(extensions_data, filename, report_id)
        except Exception as e:
            print(f"❌ Ошибка загрузки данных расширений: {e}")
            return False

    def upload_image_data(self, image_data: Dict, report_id: int) -> bool:
        """Загружает данные изображений"""
        try:
            filename = f"image_hashes_report_{report_id}.json"
            return self.upload_json_data(image_data, filename, report_id)
        except Exception as e:
            print(f"❌ Ошибка загрузки данных изображений: {e}")
            return False

    def upload_keywords_data(self, keywords_data: Dict, report_id: int) -> bool:
        """Загружает данные ключевых фраз"""
        try:
            filename = f"keywords_traffic_forecast_{report_id}.json"
            return self.upload_json_data(keywords_data, filename, report_id)
        except Exception as e:
            print(f"❌ Ошибка загрузки данных ключевых фраз: {e}")
            return False

    def upload_campaign_stats_data(self, stats_data: Dict, report_id: int) -> bool:
        """Загружает данные статистики кампаний в форматах TSV и JSON"""
        try:
            # Извлекаем TSV содержимое из данных отчета
            report_content = stats_data.get('report', '')
            if not report_content:
                print("❌ Отсутствует содержимое отчета")
                return False

            # Определяем формат файла
            report_format = stats_data.get('_meta', {}).get('format', 'TSV')
            success = True

            if report_format == 'TSV':
                # Сохраняем TSV файл
                tsv_filename = f"campaign_stats_{report_id}.tsv"
                tsv_result = self.upload_tsv_data(report_content, tsv_filename, report_id)
                success = success and tsv_result

                # Преобразуем TSV в JSON и сохраняем
                json_data = self.convert_tsv_to_json(report_content)
                if json_data:
                    json_filename = f"campaign_stats_{report_id}.json"
                    json_result = self.upload_json_data(json_data, json_filename, report_id)
                    success = success and json_result
                else:
                    print("⚠️ Не удалось преобразовать TSV в JSON")
                    success = False
            else:
                # Для JSON формата используем старый метод
                filename = f"campaign_stats_{report_id}.json"
                success = self.upload_json_data(stats_data, filename, report_id)

            return success

        except Exception as e:
            print(f"❌ Ошибка загрузки данных статистики кампаний: {e}")
            return False

    def upload_memory_file(self, file_name: str, data: str, length: int):
        self.client.put_object(self.bucket_name, file_name, data, length)

    def convert_tsv_to_json(self, tsv_content: str) -> Dict:
        """Преобразует TSV содержимое в JSON структуру"""
        try:
            lines = tsv_content.strip().split('\n')

            # Пропускаем первую строку с заголовком отчета
            # Пропускаем вторую строку с названиями колонок
            data_lines = lines[2:]  # Начинаем с третьей строки

            # Удаляем последнюю строку с "Total rows: X" если есть
            if data_lines and data_lines[-1].startswith('Total rows:'):
                data_lines = data_lines[:-1]

            # Удаляем пустые строки
            data_lines = [line.strip() for line in data_lines if line.strip()]

            # Парсим данные
            rows = []
            for line in data_lines:
                fields = line.split('\t')
                if len(fields) >= 6:  # CampaignId, CampaignName, Impressions, Clicks, Ctr, BounceRate
                    row = {
                        "CampaignId": int(fields[0]) if fields[0].isdigit() else None,
                        "CampaignName": fields[1],
                        "Impressions": int(fields[2]) if fields[2].isdigit() else 0,
                        "Clicks": int(fields[3]) if fields[3].isdigit() else 0,
                        "Ctr": float(fields[4]) if fields[4].replace('.', '').isdigit() else 0.0,
                        "BounceRate": float(fields[5]) if fields[5].replace('.', '').isdigit() else 0.0
                    }
                    rows.append(row)

            # Создаем JSON структуру
            result = {
                "result": {
                    "rows": rows
                },
                "_meta": {
                    "total_rows": len(rows),
                    "format": "JSON",
                    "source": "TSV"
                }
            }

            return result

        except Exception as e:
            print(f"❌ Ошибка преобразования TSV в JSON: {e}")
            return None

    def list_objects(self, prefix: str = None) -> List[str]:
        """Получает список объектов в bucket"""
        try:
            if not self.client:
                return []

            objects = self.client.list_objects(
                bucket_name=self.bucket_name,
                prefix=prefix or self.base_path,
                recursive=True
            )

            object_names = []
            for obj in objects:
                object_names.append(obj.object_name)

            return object_names

        except Exception as e:
            print(f"❌ Ошибка получения списка объектов: {e}")
            return []

    def get_object_info(self, object_name: str) -> Optional[Dict]:
        """Получает информацию об объекте"""
        try:
            if not self.client:
                return None

            stat = self.client.stat_object(self.bucket_name, object_name)
            return {
                'object_name': object_name,
                'size': stat.size,
                'last_modified': stat.last_modified,
                'etag': stat.etag,
                'content_type': stat.content_type
            }

        except Exception as e:
            print(f"❌ Ошибка получения информации об объекте: {e}")
            return None

    def upload_campaign_stats_summary_data(self, summary_data: Dict, report_id: int) -> bool:
        """Загружает сводные данные статистики кампаний в форматах TSV и JSON"""
        try:
            # Извлекаем TSV содержимое из данных отчета
            report_content = summary_data.get('report', '')
            if not report_content:
                print("❌ Отсутствует содержимое сводного отчета")
                return False

            # Определяем формат файла
            report_format = summary_data.get('_meta', {}).get('format', 'TSV')
            success = True

            if report_format == 'TSV':
                # Сохраняем TSV файл
                tsv_filename = f"campaign_stats_summary_{report_id}.tsv"
                tsv_result = self.upload_tsv_data(report_content, tsv_filename, report_id)
                success = success and tsv_result

                # Преобразуем TSV в JSON и сохраняем
                json_data = self.convert_tsv_summary_to_json(report_content)
                if json_data:
                    json_filename = f"campaign_stats_summary_{report_id}.json"
                    json_result = self.upload_json_data(json_data, json_filename, report_id)
                    success = success and json_result
                else:
                    print("⚠️ Не удалось преобразовать сводный TSV в JSON")
                    success = False
            else:
                # Для JSON формата используем старый метод
                filename = f"campaign_stats_summary_{report_id}.json"
                success = self.upload_json_data(summary_data, filename, report_id)

            return success

        except Exception as e:
            print(f"❌ Ошибка загрузки сводных данных статистики кампаний: {e}")
            return False

    def _is_numeric(self, value: str) -> bool:
        """Проверяет, является ли строка числом (включая десятичные)"""
        if not value or value == '--':
            return False
        try:
            float(value)
            return True
        except ValueError:
            return False

    def convert_tsv_summary_to_json(self, tsv_content: str) -> Dict:
        """Преобразует сводное TSV содержимое в JSON структуру"""
        try:
            lines = tsv_content.strip().split('\n')

            # Пропускаем первую строку с заголовком отчета
            # Пропускаем вторую строку с названиями колонок
            data_lines = lines[2:]  # Начинаем с третьей строки

            # Удаляем последнюю строку с "Total rows: X" если есть
            if data_lines and data_lines[-1].startswith('Total rows:'):
                data_lines = data_lines[:-1]

            # Удаляем пустые строки
            data_lines = [line.strip() for line in data_lines if line.strip()]

            # Парсим данные (сводный отчет по аккаунту - одна строка с агрегированными данными)
            summary_row = None

            if data_lines:
                # Парсим единственную строку с агрегированными данными по аккаунту
                fields = data_lines[0].split('\t')
                if len(fields) >= 6:  # Impressions, Clicks, Ctr, BounceRate, Cost, AvgCpc
                    summary_row = {
                        "Impressions": int(fields[0]) if fields[0].isdigit() else 0,
                        "Clicks": int(fields[1]) if fields[1].isdigit() else 0,
                        "Ctr": float(fields[2]) if self._is_numeric(fields[2]) else 0.0,
                        "BounceRate": float(fields[3]) if self._is_numeric(fields[3]) else 0.0,
                        "Cost": float(fields[4]) if self._is_numeric(fields[4]) else 0.0,
                        "AvgCpc": float(fields[5]) if self._is_numeric(fields[5]) else 0.0
                    }

            # Создаем JSON структуру
            result = {
                "summary": summary_row,
                "_meta": {
                    "type": "summary",
                    "format": "JSON",
                    "source": "TSV",
                    "report_type": "ACCOUNT_PERFORMANCE_REPORT"
                }
            }

            return result

        except Exception as e:
            print(f"❌ Ошибка преобразования сводного TSV в JSON: {e}")
            return None

    def upload_ad_stats_data(self, stats_data: Dict, report_id: int) -> bool:
        """Загружает данные статистики объявлений в форматах TSV и JSON"""
        try:
            # Извлекаем TSV содержимое из данных отчета
            report_content = stats_data.get('report', '')
            if not report_content:
                print("❌ Отсутствует содержимое отчета по объявлениям")
                return False

            # Определяем формат файла
            report_format = stats_data.get('_meta', {}).get('format', 'TSV')
            success = True

            if report_format == 'TSV':
                # Сохраняем TSV файл
                tsv_filename = f"ad_stats_{report_id}.tsv"
                tsv_result = self.upload_tsv_data(report_content, tsv_filename, report_id)
                success = success and tsv_result

                # Преобразуем TSV в JSON и сохраняем
                json_data = self.convert_ad_stats_tsv_to_json(report_content)
                if json_data:
                    json_filename = f"ad_stats_{report_id}.json"
                    json_result = self.upload_json_data(json_data, json_filename, report_id)
                    success = success and json_result
                else:
                    print("⚠️ Не удалось преобразовать TSV отчета по объявлениям в JSON")
                    success = False
            else:
                # Для JSON формата используем старый метод
                filename = f"ad_stats_{report_id}.json"
                success = self.upload_json_data(stats_data, filename, report_id)

            return success

        except Exception as e:
            print(f"❌ Ошибка загрузки данных статистики объявлений: {e}")
            return False

    def convert_ad_stats_tsv_to_json(self, tsv_content: str) -> Dict:
        """Преобразует TSV содержимое отчета по объявлениям в JSON структуру"""
        try:
            lines = tsv_content.strip().split('\n')

            # Пропускаем первую строку с заголовком отчета
            # Пропускаем вторую строку с названиями колонок
            data_lines = lines[2:]  # Начинаем с третьей строки

            # Удаляем последнюю строку с "Total rows: X" если есть
            if data_lines and data_lines[-1].startswith('Total rows:'):
                data_lines = data_lines[:-1]

            # Удаляем пустые строки
            data_lines = [line.strip() for line in data_lines if line.strip()]

            # Парсим данные
            rows = []
            for line in data_lines:
                fields = line.split('\t')
                if len(fields) >= 8:  # CampaignId, AdId, Impressions, Clicks, Ctr, BounceRate, Cost, AvgCpc
                    row = {
                        "CampaignId": int(fields[0]) if fields[0].isdigit() else None,
                        "AdId": int(fields[1]) if fields[1].isdigit() else None,
                        "Impressions": int(fields[2]) if fields[2].isdigit() else 0,
                        "Clicks": int(fields[3]) if fields[3].isdigit() else 0,
                        "Ctr": float(fields[4]) if self._is_numeric(fields[4]) else 0.0,
                        "BounceRate": float(fields[5]) if self._is_numeric(fields[5]) else 0.0,
                        "Cost": float(fields[6]) if self._is_numeric(fields[6]) else 0.0,
                        "AvgCpc": float(fields[7]) if self._is_numeric(fields[7]) else 0.0
                    }
                    rows.append(row)

            # Создаем JSON структуру
            result = {
                "result": {
                    "rows": rows
                },
                "_meta": {
                    "total_rows": len(rows),
                    "format": "JSON",
                    "source": "TSV",
                    "report_type": "AD_PERFORMANCE_REPORT"
                }
            }

            return result

        except Exception as e:
            print(f"❌ Ошибка преобразования TSV отчета по объявлениям в JSON: {e}")
            return None

    def upload_adgroup_stats_data(self, stats_data: Dict, report_id: int) -> bool:
        """Загружает данные статистики групп объявлений в форматах TSV и JSON"""
        try:
            # Извлекаем TSV содержимое из данных отчета
            report_content = stats_data.get('report', '')
            if not report_content:
                print("❌ Отсутствует содержимое отчета по группам объявлений")
                return False

            # Определяем формат файла
            report_format = stats_data.get('_meta', {}).get('format', 'TSV')
            success = True

            if report_format == 'TSV':
                # Сохраняем TSV файл
                tsv_filename = f"adgroup_stats_{report_id}.tsv"
                tsv_result = self.upload_tsv_data(report_content, tsv_filename, report_id)
                success = success and tsv_result

                # Преобразуем TSV в JSON и сохраняем
                json_data = self.convert_adgroup_stats_tsv_to_json(report_content)
                if json_data:
                    json_filename = f"adgroup_stats_{report_id}.json"
                    json_result = self.upload_json_data(json_data, json_filename, report_id)
                    success = success and json_result
                else:
                    print("⚠️ Не удалось преобразовать TSV отчета по группам объявлений в JSON")
                    success = False
            else:
                # Для JSON формата используем старый метод
                filename = f"adgroup_stats_{report_id}.json"
                success = self.upload_json_data(stats_data, filename, report_id)

            return success

        except Exception as e:
            print(f"❌ Ошибка загрузки данных статистики групп объявлений: {e}")
            return False

    def convert_adgroup_stats_tsv_to_json(self, tsv_content: str) -> Dict:
        """Преобразует TSV содержимое отчета по группам объявлений в JSON структуру"""
        try:
            lines = tsv_content.strip().split('\n')

            # Пропускаем первую строку с заголовком отчета
            # Пропускаем вторую строку с названиями колонок
            data_lines = lines[2:]  # Начинаем с третьей строки

            # Удаляем последнюю строку с "Total rows: X" если есть
            if data_lines and data_lines[-1].startswith('Total rows:'):
                data_lines = data_lines[:-1]

            # Удаляем пустые строки
            data_lines = [line.strip() for line in data_lines if line.strip()]

            # Парсим данные
            rows = []
            for line in data_lines:
                fields = line.split('\t')
                if len(fields) >= 11:  # CampaignId, AdGroupId, AdGroupName, CampaignType, AdNetworkType, Impressions, Clicks, Ctr, BounceRate, Cost, AvgCpc
                    row = {
                        "CampaignId": int(fields[0]) if fields[0].isdigit() else None,
                        "AdGroupId": int(fields[1]) if fields[1].isdigit() else None,
                        "AdGroupName": fields[2] if fields[2] else "",
                        "CampaignType": fields[3] if fields[3] else "",
                        "AdNetworkType": fields[4] if fields[4] else "",
                        "Impressions": int(fields[5]) if fields[5].isdigit() else 0,
                        "Clicks": int(fields[6]) if fields[6].isdigit() else 0,
                        "Ctr": float(fields[7]) if self._is_numeric(fields[7]) else 0.0,
                        "BounceRate": float(fields[8]) if self._is_numeric(fields[8]) else 0.0,
                        "Cost": float(fields[9]) if self._is_numeric(fields[9]) else 0.0,
                        "AvgCpc": float(fields[10]) if self._is_numeric(fields[10]) else 0.0
                    }
                    rows.append(row)

            # Создаем JSON структуру
            result = {
                "result": {
                    "rows": rows
                },
                "_meta": {
                    "total_rows": len(rows),
                    "format": "JSON",
                    "source": "TSV",
                    "report_type": "ADGROUP_PERFORMANCE_REPORT"
                }
            }

            return result

        except Exception as e:
            print(f"❌ Ошибка преобразования TSV отчета по группам объявлений в JSON: {e}")
            return None

    def download_ads_report_json(self, report_id: str) -> Optional[Dict]:
        """Скачивает JSON файл с данными объявлений из MinIO"""
        try:
            # Формируем путь к файлу в MinIO
            prefix = f"reports/{report_id}/"

            # Ищем JSON файл с объявлениями
            objects = self.client.list_objects(
                self.bucket_name,
                prefix=prefix,
                recursive=True
            )

            json_file = None
            for obj in objects:
                if obj.object_name.endswith('_ads.json'):
                    json_file = obj.object_name
                    break

            if not json_file:
                print(f"❌ JSON файл с объявлениями не найден для отчета {report_id}")
                return None

            # Скачиваем файл
            response = self.client.get_object(self.bucket_name, json_file)
            content = response.read().decode('utf-8')
            response.close()
            response.release_conn()

            # Парсим JSON
            data = json.loads(content)
            print(f"✅ JSON файл с объявлениями скачан: {json_file}")
            return data

        except Exception as e:
            print(f"❌ Ошибка скачивания JSON файла с объявлениями: {e}")
            return None