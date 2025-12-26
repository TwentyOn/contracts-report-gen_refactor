#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Главный файл для централизованного управления всеми скриптами
Запускает все скрипты по очереди в правильном порядке
"""
import io
import os
import json
import time
from typing import Dict, List, Optional
import logging

from dotenv import load_dotenv

from database_manager import DatabaseManager
from api_client import DirectAPIClient
from minio_client import MinIOClient
from get_campaigns_data_refactored import CampaignsDataProcessor
from get_adgroups_data_refactored import AdGroupsDataProcessor
from generate_report_urls_refactored import ReportURLGenerator
from generate_screenshots_refactored import ScreenshotGenerator
from ad_screenshots_very_good_generator import very_good_screenshot_generator

from generate_report_files.soprovod_generator import generate_soprovod
from generate_report_files.act_generator import generate_act
from generate_report_files.statement_generator import generate_vedomost
from generate_report_files.screen_ads.ad_screenshots_generator import generate_screens_ads
from generate_report_files.presentation.presentation_generator import generate_presentation
from generate_report_files.media_plan.mediaplan_generator import generate_mediaplan
from generate_report_files.report_generator import word_report_generate

from utils.postprocessing_report_file import upload_to_s3, write_s3path_to_bd, write_status, all_reports_zip_create

# Загружаем переменные окружения
load_dotenv('.env')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('main_processor.py')


class MainProcessor:
    """Главный процессор для управления всеми скриптами"""

    def __init__(self):
        self.db = DatabaseManager()
        self.minio_client = MinIOClient()
        self.current_account = None
        self.current_client_login = None
        self.current_report_id = None

    def run_all_scripts(self):
        """Запускает все скрипты по очереди"""
        print("🚀 Запуск централизованной обработки всех скриптов")
        print("=" * 80)

        # Подключаемся к БД
        if not self.db.connect():
            print("❌ Не удалось подключиться к БД")
            return False

        # Подключаемся к MinIO
        if not self.minio_client.connect():
            print("❌ Не удалось подключиться к MinIO")
            return False

        try:
            # Получаем данные для обработки
            reports = self.db.get_reports_to_process()
            if not reports:
                print("ℹ️ Нет отчетов для обработки")
                return True

            # Получаем аккаунты
            yandex_accounts = self.db.get_yandex_accounts()
            wordstat_accounts = self.db.get_wordstat_accounts()

            if not yandex_accounts:
                print("❌ Не найдены аккаунты Яндекс.Директ")
                return False

            # Обрабатываем каждый отчет
            for report in reports:
                print(f"\n📋 Обработка отчета ID: {report['id']}")
                print("-" * 60)

                # Устанавливаем текущий ID отчета
                self.current_report_id = report['id']

                success = self.process_single_report(report, yandex_accounts, wordstat_accounts)
                if not success:
                    print(f"❌ Ошибка обработки отчета {report['id']}")
                    continue

                print(f"✅ Отчет {report['id']} обработан успешно")

            return True

        except Exception:
            raise

        finally:
            self.db.disconnect()

    def process_single_report(self, report: Dict, yandex_accounts: List[Dict],
                              wordstat_accounts: List[Dict]) -> bool:
        """Обрабатывает один отчет всеми скриптами по очереди"""
        try:
            # Получаем данные заявки и договора
            request_data = self.db.get_request_data(report['id_requests'])
            contract_data = self.db.get_contract_data(report['id_contracts'])

            if not request_data or not contract_data:
                print("❌ Не найдены данные заявки или договора")
                return False

            # Извлекаем ID кампаний
            campaign_ids = self.db.extract_campaign_ids(request_data.get('campany_yandex_direct'))
            if not campaign_ids:
                print("❌ Не найдены ID кампаний")
                return False

            print(f"📊 Найдено кампаний: {len(campaign_ids)}")
            print(f"📊 ID кампаний: {campaign_ids}")

            # Настраиваем API клиент
            if not self.setup_api_client(yandex_accounts, contract_data):
                print("❌ Не удалось настроить API клиент")
                return False

            # статус отчёта 2 - в обработке
            write_status(report['id'], 2)

            # 1. Получаем данные о кампаниях (get_campaigns_data)
            print("\n🔹 Шаг 1: Получение данных о кампаниях")
            campaigns_processor = CampaignsDataProcessor()
            campaigns_processor.api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )
            campaigns_data = campaigns_processor.get_campaigns_data()
            if not campaigns_data:
                print("❌ Ошибка получения данных о кампаниях")
                return False

            # Сохраняем данные в MinIO
            success = self.minio_client.upload_json_data(
                campaigns_data,
                "campaigns.json",
                report['id']
            )
            if not success:
                print("❌ Ошибка сохранения данных в MinIO")
                return False

            # 2. Получаем группы объявлений (get_adgroups_data)
            print("\n🔹 Шаг 2: Получение групп объявлений")
            adgroups_processor = AdGroupsDataProcessor()
            adgroups_processor.api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )
            adgroups_processor.minio_client = self.minio_client

            # Получаем удаленные группы для исключения
            deleted_group_ids = self.get_deleted_groups(request_data)

            adgroups_data = adgroups_processor.get_adgroups_data(campaign_ids, deleted_group_ids)
            if not adgroups_data:
                print("❌ Ошибка получения данных о группах")
                return False

            # Сохраняем данные в MinIO
            adgroups_processor.save_adgroups_data(adgroups_data, report)

            # 3. Получаем объявления (get_campaign_ads)
            print("\n🔹 Шаг 3: Получение объявлений")
            ads_success = self.get_campaign_ads(campaign_ids, report, request_data, contract_data)
            if not ads_success:
                print("❌ Ошибка получения объявлений")
                return False

            # 4. Получаем расширения и быстрые ссылки (get_extensions_and_sitelinks)
            print("\n🔹 Шаг 4: Получение расширений и быстрых ссылок")
            extensions_success = self.get_extensions_and_sitelinks(report)
            if not extensions_success:
                print("⚠️ Ошибка получения расширений, продолжаем...")

            # 5. Получаем хеши изображений (get_image_hashes_from_report)
            print("\n🔹 Шаг 5: Получение хешей изображений")
            images_success = self.get_image_hashes_from_report(report)
            if not images_success:
                print("⚠️ Ошибка получения хешей изображений, продолжаем...")

            # 6. Получаем прогнозы трафика (get_keywords_traffic_forecast)
            print("\n🔹 Шаг 6: Получение прогнозов трафика")
            keywords_success = self.get_keywords_traffic_forecast(campaign_ids, report, request_data, contract_data)
            if not keywords_success:
                print("⚠️ Ошибка получения прогнозов трафика, продолжаем...")

            # 7. Получаем статистику по кампаниям (get_campaign_stats)
            print("\n🔹 Шаг 7: Получение статистики по кампаниям")
            stats_success = self.get_campaign_stats(campaign_ids, report, request_data, contract_data)
            if not stats_success:
                print("⚠️ Ошибка получения статистики по кампаниям, продолжаем...")

            # 8. Получаем статистику по объявлениям (get_ad_stats)
            print("\n🔹 Шаг 8: Получение статистики по объявлениям")
            ad_stats_success = self.get_ad_stats(campaign_ids, report, request_data, contract_data)
            if not ad_stats_success:
                print("⚠️ Ошибка получения статистики по объявлениям, продолжаем...")

            # 9. Получаем статистику по группам объявлений (get_adgroup_stats)
            print("\n🔹 Шаг 9: Получение статистики по группам объявлений")
            adgroup_stats_success = self.get_adgroup_stats(campaign_ids, report, request_data, contract_data)
            if not adgroup_stats_success:
                print("⚠️ Ошибка получения статистики по группам объявлений, продолжаем...")

            # 10. Обрабатываем Wordstat данные (get_wordstat_data)
            print("\n🔹 Шаг 10: Обработка Wordstat данных")
            wordstat_success = self.get_wordstat_data(wordstat_accounts)
            if not wordstat_success:
                print("⚠️ Ошибка обработки Wordstat данных, продолжаем...")

            # 11. Генерируем URL отчетов (generate_report_urls)
            print("\n🔹 Шаг 11: Генерация URL отчетов")
            urls_success = self.generate_report_urls(report, request_data, contract_data, campaign_ids)
            if not urls_success:
                print("⚠️ Ошибка генерации URL отчетов, продолжаем...")

            # 12. Генерируем скриншоты отчетов (generate_screenshots)
            print("\n🔹 Шаг 12: Генерация скриншотов отчетов")
            screenshots_success = self.generate_screenshots(report)
            if not screenshots_success:
                print("⚠️ Ошибка генерации скриншотов, продолжаем...")

            # 13. Генерация very_good_ads
            logger.info('Шаг 13: Формирую very_good_ads...')
            very_good_screenshot_generator(self.current_report_id)

            # 14. Генерация файлов-отчётов
            # print('Формирование файлов отчёта...')
            logger.info('Шаг 14: Формирую файлы отчётов...')

            # сопровод
            soprovod_file, soprovod_filename = generate_soprovod(self.current_report_id)
            soprovod_path_s3 = upload_to_s3(soprovod_file, soprovod_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('SOPROVOD_COL_NAME'), soprovod_path_s3)

            # акт
            act_file, act_filename = generate_act(self.current_report_id)
            act_path_s3 = upload_to_s3(act_file, act_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('ACT_COL_NAME'), act_path_s3)

            # ведомость
            vegomost_file, vedomost_filename = generate_vedomost(self.current_report_id)
            vedomost_path_s3 = upload_to_s3(vegomost_file, vedomost_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('VEDOMOST_COL_NAME'), vedomost_path_s3)

            # архив со скриншотами объявлений
            screens_file, screens_filename = generate_screens_ads(self.current_report_id)
            screens_path_s3 = upload_to_s3(screens_file, screens_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('SCREENSHOTS_COL_NAME'), screens_path_s3)

            # презентация
            pres_file, pres_filename = generate_presentation(self.current_report_id)
            pres_path_s3 = upload_to_s3(pres_file, pres_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('PRESENTATION_COL_NAME'), pres_path_s3)
            #
            # медиаплан
            mediaplan_file, mediaplan_filename = generate_mediaplan(self.current_report_id)
            mediaplan_path_s3 = upload_to_s3(mediaplan_file, mediaplan_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('MEDIAPLAN_COL_NAME'), mediaplan_path_s3)

            # отчёт
            workreport_file, wordreport_filename = word_report_generate(self.current_report_id)
            workreport_path_s3 = upload_to_s3(workreport_file, wordreport_filename)
            write_s3path_to_bd(self.current_report_id, os.getenv('CONTENT_REPORT_COL_NAME'), workreport_path_s3)

            # архив со всеми файлами
            all_reports_zip, zip_name = all_reports_zip_create(self.current_report_id,
                                                               (soprovod_file, soprovod_filename),
                                                               (act_file, act_filename),
                                                               (vegomost_file, vedomost_filename),
                                                               (screens_file, screens_filename),
                                                               (pres_file, pres_filename),
                                                               (mediaplan_file, mediaplan_filename),
                                                               (workreport_file, wordreport_filename))
            all_reports_path = upload_to_s3(all_reports_zip, zip_name)
            write_s3path_to_bd(self.current_report_id, os.getenv('ALL_REPORT_ZIP'), all_reports_path)

            # статус обработки 3 - завершено
            write_status(self.current_report_id, 3)

            print(f"\n✅ Обработка отчета {report['id']} завершена")
            return True

        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")
            write_status(self.current_report_id, 4, str(e))
            raise e
            return False

    def setup_api_client(self, accounts: List[Dict], contract_data: Dict) -> bool:
        """Настраивает API клиент с правильным аккаунтом"""
        try:
            # Используем логин из договора, если он есть
            client_login = contract_data.get('login_yandex_direct')
            if client_login:
                print(f"✅ Используем логин из договора: {client_login}")
            else:
                print(f"⚠️ Логин из договора не найден")
                client_login = None

            # Пытаемся найти рабочий аккаунт
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
                api_client = DirectAPIClient(
                    account['direct_api_token'],
                    client_login
                )

                # Тестируем подключение
                if api_client.test_connection():
                    print("✅ Подключение к API успешно")
                    self.current_account = account
                    self.current_client_login = client_login
                    return True
                else:
                    print("❌ Ошибка подключения к API")

                # Небольшая пауза между попытками
                time.sleep(2)

            return False

        except Exception as e:
            print(f"❌ Ошибка настройки API клиента: {e}")
            return False

    def get_campaign_ads(self, campaign_ids: List[int], report: Dict,
                         request_data: Dict, contract_data: Dict) -> bool:
        """Получает объявления по кампаниям"""
        try:
            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Получаем объявления
            ads_data = api_client.get_ads_by_campaigns(campaign_ids)
            if not ads_data:
                print("❌ Не удалось получить объявления")
                return False

            # Получаем удаленные группы для исключения
            deleted_group_ids = self.get_deleted_groups(request_data)

            # Фильтруем объявления по удаленным группам
            filtered_ads_data = self.filter_ads_by_deleted_groups(ads_data, deleted_group_ids)

            # Сохраняем данные
            self.save_ads_data(filtered_ads_data, report, request_data, contract_data)
            return True

        except Exception as e:
            print(f"❌ Ошибка получения объявлений: {e}")
            return False

    def get_extensions_and_sitelinks(self, report: Dict) -> bool:
        """Получает расширения и быстрые ссылки"""
        try:
            # Находим последний файл ads_report
            ads_data = self.find_latest_ads_report(report['id'])
            if not ads_data:
                print("❌ Не найден файл ads_report")
                return False

            # Извлекаем уникальные ID
            unique_ids = self.extract_unique_ids(ads_data)
            sitelink_set_ids = unique_ids['sitelink_set_ids']
            extension_ids = unique_ids['extension_ids']

            if not sitelink_set_ids and not extension_ids:
                print("⚠️ Не найдены SitelinkSetId и AdExtensionId для скачивания")
                return True

            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Скачиваем быстрые ссылки
            sitelinks_data = {}
            if sitelink_set_ids:
                for sitelink_id in sitelink_set_ids:
                    sitelink_data = api_client.get_sitelinks_by_set_id(sitelink_id)
                    if sitelink_data:
                        sitelinks_data[sitelink_id] = sitelink_data

            # Скачиваем расширения
            extensions_data = {}
            if extension_ids:
                # Разбиваем на батчи по 1000 ID
                extension_ids_list = list(extension_ids)
                batch_size = 1000

                for i in range(0, len(extension_ids_list), batch_size):
                    batch = extension_ids_list[i:i + batch_size]
                    extensions_data_batch = api_client.get_extensions_by_ids(batch)
                    if extensions_data_batch:
                        extensions_data[f'batch_{i // batch_size + 1}'] = extensions_data_batch

            # Сохраняем данные
            if sitelinks_data:
                self.save_sitelinks_data(sitelinks_data)
            if extensions_data:
                self.save_extensions_data(extensions_data)

            return True

        except Exception as e:
            print(f"❌ Ошибка получения расширений: {e}")
            return False

    def get_image_hashes_from_report(self, report: Dict) -> bool:
        """Получает хеши изображений из отчета"""
        try:
            # Находим последний файл ads_report
            ads_data = self.find_latest_ads_report(report['id'])
            if not ads_data:
                print("❌ Не найден файл ads_report")
                return False

            # Извлекаем уникальные хеши
            unique_hashes = self.extract_unique_image_hashes(ads_data)
            if not unique_hashes:
                print("❌ Уникальных хешей изображений не найдено")
                return True

            print(f"✅ Найдено уникальных хешей: {len(unique_hashes)}")

            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Получаем данные изображений
            image_data = api_client.get_image_urls_by_hashes(list(unique_hashes))
            if not image_data:
                print("❌ Не удалось получить данные изображений")
                return False

            # Сохраняем данные
            self.save_image_data(image_data, unique_hashes, report['id'])
            return True

        except Exception as e:
            print(f"❌ Ошибка получения хешей изображений: {e}")
            return False

    def get_keywords_traffic_forecast(self, campaign_ids: List[int], report: Dict,
                                      request_data: Dict, contract_data: Dict) -> bool:
        """Получает прогнозы трафика по ключевым фразам"""
        try:
            # Загружаем группы из MinIO
            prefix = f"gen_report_context_contracts/data_yandex_direct/{report['id']}_результаты/"
            adgroups_file = f"{prefix}adgroups_{report['id']}.json"

            try:
                response = self.minio_client.client.get_object(
                    self.minio_client.bucket_name,
                    adgroups_file
                )
                adgroups_data = json.loads(response.read().decode('utf-8'))
                response.close()
                response.release_conn()
            except Exception as e:
                print(f"❌ Ошибка загрузки групп из MinIO: {e}")
                return False

            if not adgroups_data or 'result' not in adgroups_data or 'AdGroups' not in adgroups_data['result']:
                print("❌ Неверный формат данных групп в MinIO")
                return False

            # Берем только активные группы
            adgroup_ids = [ag['Id'] for ag in adgroups_data['result']['AdGroups'] if ag.get('Status') == 'ACCEPTED']
            print(f"✅ Найдено групп объявлений: {len(adgroup_ids)}")

            if not adgroup_ids:
                print("❌ Группы объявлений не найдены")
                return False

            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Получаем ключевые фразы
            keywords_data = api_client.get_keywords_by_adgroups(adgroup_ids)
            if not keywords_data:
                print("❌ Не удалось получить ключевые фразы")
                return False

            # Сохраняем данные
            self.save_keywords_data(keywords_data, report)
            return True

        except Exception as e:
            print(f"❌ Ошибка получения прогнозов трафика: {e}")
            return False

    def get_campaign_stats(self, campaign_ids: List[int], report: Dict,
                           request_data: Dict, contract_data: Dict) -> bool:
        """Получает статистику по кампаниям"""
        try:
            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Получаем даты из заявки
            start_date, end_date = self.get_report_dates(request_data)
            if not start_date or not end_date:
                print("❌ Не найдены даты начала и окончания")
                return False

            print(f"📅 Период отчета: {start_date} - {end_date}")

            # Получаем удаленные группы для исключения
            deleted_group_ids = self.get_deleted_groups(request_data)

            # Создаем основной отчет с учетом удаленных групп
            if deleted_group_ids:
                print(f"🔧 Используем кастомный отчет с фильтрацией по группам")
                report_data = api_client.create_custom_campaign_report_with_group_filter(
                    campaign_ids, start_date, end_date, deleted_group_ids
                )
            else:
                print(f"🔧 Используем стандартный отчет по кампаниям")
                report_data = api_client.create_campaign_performance_report(
                    campaign_ids, start_date, end_date
                )

            if not report_data:
                print("❌ Не удалось создать отчет")
                return False

            # Сохраняем основные данные
            success = self.minio_client.upload_campaign_stats_data(report_data, report['id'])
            if not success:
                print(f"❌ Ошибка сохранения данных статистики кампаний в MinIO")
                return False

            print(f"💾 Данные статистики кампаний сохранены в MinIO для отчета {report['id']}")

            # Создаем саммари-отчет с учетом удаленных групп
            print("📊 Создание саммари-отчета...")
            if deleted_group_ids:
                print(f"🔧 Используем кастомный сводный отчет с фильтрацией по группам")
                summary_data = api_client.create_custom_campaign_summary_report_with_group_filter(
                    campaign_ids, start_date, end_date, deleted_group_ids
                )
            else:
                print(f"🔧 Используем стандартный сводный отчет по кампаниям")
                summary_data = api_client.create_campaign_performance_summary_report(
                    campaign_ids, start_date, end_date
                )

            if summary_data:
                print("✅ Саммари-отчет получен успешно")
                # Сохраняем саммари-данные
                summary_success = self.minio_client.upload_campaign_stats_summary_data(
                    summary_data, report['id']
                )
                if summary_success:
                    print(f"💾 Саммари-данные статистики кампаний сохранены в MinIO для отчета {report['id']}")
                else:
                    print(f"❌ Ошибка сохранения саммари-данных статистики кампаний в MinIO")
            else:
                print("⚠️ Не удалось получить саммари-отчет")

            return True

        except Exception as e:
            print(f"❌ Ошибка получения статистики по кампаниям: {e}")
            return False

    def get_ad_stats(self, campaign_ids: List[int], report: Dict,
                     request_data: Dict, contract_data: Dict) -> bool:
        """Получает статистику по объявлениям"""
        try:
            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Получаем даты из заявки
            start_date, end_date = self.get_report_dates(request_data)
            if not start_date or not end_date:
                print("❌ Не найдены даты начала и окончания")
                return False

            print(f"📅 Период отчета: {start_date} - {end_date}")

            # Получаем удаленные группы для исключения
            deleted_group_ids = self.get_deleted_groups(request_data)

            # Создаем отчет с учетом удаленных групп
            report_data = api_client.create_ad_performance_report(
                campaign_ids, start_date, end_date, deleted_group_ids
            )

            if not report_data:
                print("❌ Не удалось создать отчет по объявлениям")
                return False

            # Данные уже обработаны API, сохраняем их напрямую
            success = self.minio_client.upload_ad_stats_data(report_data, report['id'])
            if success:
                print(f"💾 Данные статистики объявлений сохранены в MinIO для отчета {report['id']}")
                return True
            else:
                print(f"❌ Ошибка сохранения данных статистики объявлений в MinIO")
                return False

        except Exception as e:
            print(f"❌ Ошибка получения статистики по объявлениям: {e}")
            return False

    def get_adgroup_stats(self, campaign_ids: List[int], report: Dict,
                          request_data: Dict, contract_data: Dict) -> bool:
        """Получает статистику по группам объявлений"""
        try:
            # Создаем API клиент
            api_client = DirectAPIClient(
                self.current_account['direct_api_token'],
                self.current_client_login
            )

            # Получаем даты из заявки
            start_date, end_date = self.get_report_dates(request_data)
            if not start_date or not end_date:
                print("❌ Не найдены даты начала и окончания")
                return False

            print(f"📅 Период отчета: {start_date} - {end_date}")

            # Получаем удаленные группы для исключения
            deleted_group_ids = self.get_deleted_groups(request_data)

            # Создаем отчет с учетом удаленных групп
            report_data = api_client.create_adgroup_performance_report(
                campaign_ids, start_date, end_date, deleted_group_ids
            )

            if not report_data:
                print("❌ Не удалось создать отчет по группам объявлений")
                return False

            # Данные уже обработаны API, сохраняем их напрямую
            success = self.minio_client.upload_adgroup_stats_data(report_data, report['id'])
            if success:
                print(f"💾 Данные статистики групп объявлений сохранены в MinIO для отчета {report['id']}")
                return True
            else:
                print(f"❌ Ошибка сохранения данных статистики групп объявлений в MinIO")
                return False

        except Exception as e:
            print(f"❌ Ошибка получения статистики по группам объявлений: {e}")
            return False

    def get_report_dates(self, request_data: Dict) -> tuple:
        """Получает даты начала и окончания из данных заявки"""
        try:
            # Получаем данные заявки из БД
            request_id = request_data['id']

            query = """
                SELECT start_date, end_date
                FROM gen_report_context_contracts.requests 
                WHERE id = %s
            """
            self.db.cursor.execute(query, (request_id,))
            row = self.db.cursor.fetchone()

            if row:
                start_date = row[0]
                end_date = row[1]

                if start_date and end_date:
                    # Форматируем даты в нужный формат YYYY-MM-DD
                    start_date_str = start_date.strftime("%Y-%m-%d")
                    end_date_str = end_date.strftime("%Y-%m-%d")
                    return start_date_str, end_date_str

            return None, None

        except Exception as e:
            print(f"❌ Ошибка получения дат: {e}")
            return None, None

    def get_deleted_groups(self, request_data: Dict) -> List[int]:
        """Получает список удаленных групп из поля deleted_groups"""
        try:
            deleted_groups_data = request_data.get('deleted_groups')
            print(f"🔍 Отладка: deleted_groups_data = {deleted_groups_data}")
            print(f"🔍 Отладка: тип данных = {type(deleted_groups_data)}")

            if not deleted_groups_data:
                print("🔍 Отладка: deleted_groups_data пустое или None")
                return []

            # Если это строка JSON, парсим её
            if isinstance(deleted_groups_data, str):
                print("🔍 Отладка: парсим JSON строку")
                deleted_groups_data = json.loads(deleted_groups_data)

            # Собираем все удаленные группы из всех кампаний
            all_deleted_groups = []
            if isinstance(deleted_groups_data, dict):
                print(f"🔍 Отладка: обрабатываем словарь с {len(deleted_groups_data)} кампаниями")
                for campaign_id, groups in deleted_groups_data.items():
                    print(f"🔍 Отладка: кампания {campaign_id}, группы: {groups}")
                    if isinstance(groups, list):
                        all_deleted_groups.extend(groups)
                        print(f"🔍 Отладка: добавлено {len(groups)} групп, всего: {len(all_deleted_groups)}")
            elif isinstance(deleted_groups_data, list):
                # Если данные уже в виде списка (прямо все группы)
                print("🔍 Отладка: данные уже в виде списка")
                all_deleted_groups = deleted_groups_data
                print(f"🔍 Отладка: найдено {len(all_deleted_groups)} групп")

            print(f"🔍 Отладка: итоговый список удаленных групп: {all_deleted_groups}")
            return all_deleted_groups

        except Exception as e:
            print(f"❌ Ошибка получения удаленных групп: {e}")
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

    def get_wordstat_data(self, wordstat_accounts: List[Dict]) -> bool:
        """Обрабатывает данные Wordstat с проверкой свежести"""
        # ВРЕМЕННО ОТКЛЮЧЕНО: Весь функционал закомментирован для пропуска обработки Wordstat данных
        print("ℹ️ Обработка Wordstat данных временно отключена")
        return True

        # try:
        #     if not wordstat_accounts:
        #         print("❌ Не найдены аккаунты Wordstat API")
        #         return False

        #     # Загружаем ключевые фразы из файла в MinIO
        #     keywords = self.db.load_keywords_from_minio(self.minio_client, self.current_report_id)
        #     if not keywords:
        #         print("❌ Не найдены ключевые фразы для обработки")
        #         return False

        #     print(f"📊 Найдено ключевых фраз: {len(keywords)}")

        #     # Обрабатываем каждую фразу
        #     total_processed = 0
        #     total_skipped = 0

        #     for i, keyword in enumerate(keywords, 1):
        #         print(f"\n📝 Обработка фразы {i}/{len(keywords)}: '{keyword}'")
        #         print("-" * 40)

        #         # Проверяем свежесть фразы в БД
        #         print(f"🔍 Проверяем свежесть фразы '{keyword}' в БД...")
        #         is_fresh = self.db.check_phrase_freshness(keyword)

        #         if is_fresh:
        #             print(f"✅ Фраза '{keyword}' СВЕЖАЯ - пропускаем API запрос")
        #             total_skipped += 1
        #             continue
        #         else:
        #             print(f"🔄 Фраза '{keyword}' НЕ СВЕЖАЯ или НЕ НАЙДЕНА - делаем API запрос")

        #         # Пытаемся с разными аккаунтами
        #         success = False
        #         for account in wordstat_accounts:
        #             try:
        #                 # Создаем Wordstat клиент
        #                 wordstat_client = WordstatAPIClient(
        #                     account['wordstat_token'],
        #                     account['wordstat_login']
        #                 )

        #                 # Получаем данные
        #                 result = wordstat_client.get_top_requests(keyword)
        #                 if result:
        #                     print(f"✅ Фраза '{keyword}' обработана успешно")
        #                     # Сохраняем фразы в БД
        #                     self.db.save_phrases_to_db(result, keyword)
        #                     success = True
        #                     total_processed += 1
        #                     break
        #                 else:
        #                     print(f"❌ Ошибка обработки фразы '{keyword}' с аккаунтом {account['wordstat_login']}")

        #             except Exception as e:
        #                 print(f"❌ Ошибка с аккаунтом {account['wordstat_login']}: {e}")
        #                 continue

        #         if not success:
        #             print(f"❌ Не удалось обработать фразу '{keyword}' ни с одним аккаунтом")

        #         # Небольшая пауза между запросами
        #         time.sleep(2)

        #     print(f"\n📊 ИТОГОВАЯ СТАТИСТИКА:")
        #     print(f"📝 Всего ключевых фраз: {len(keywords)}")
        #     print(f"✅ Успешно обработано: {total_processed}")
        #     print(f"⏭️ Пропущено (уже свежие): {total_skipped}")
        #     print(f"❌ Ошибок: {len(keywords) - total_processed - total_skipped}")

        #     return True

        # except Exception as e:
        #     print(f"❌ Ошибка обработки Wordstat данных: {e}")
        #     return False

    def find_latest_ads_report(self, report_id: int) -> Optional[Dict]:
        """Находит файл ads_report в MinIO для указанного отчета"""
        try:
            # Получаем список объектов с префиксом для указанного отчета
            prefix = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/"
            print(f"🔍 Поиск файлов с префиксом: {prefix}")
            objects = self.minio_client.list_objects(prefix)
            print(f"📁 Найдено объектов в MinIO: {len(objects)}")

            # Фильтруем только файлы ads_report
            ads_files = [obj for obj in objects if f"ads_report_{report_id}.json" in obj]
            print(f"📄 Найдено файлов ads_report: {len(ads_files)}")

            if not ads_files:
                print(f"❌ Файл ads_report_{report_id}.json не найден")
                print(f"📋 Доступные файлы: {objects}")
                return None

            # Берем первый найденный файл (должен быть только один)
            latest_file = ads_files[0]

            # Загружаем данные из MinIO
            try:
                response = self.minio_client.client.get_object(
                    self.minio_client.bucket_name,
                    latest_file
                )
                data = json.loads(response.read().decode('utf-8'))
                response.close()
                response.release_conn()
                return data
            except Exception as e:
                print(f"❌ Ошибка загрузки файла из MinIO: {e}")
                return None

        except Exception as e:
            print(f"❌ Ошибка поиска файла в MinIO: {e}")
            return None

    def extract_unique_ids(self, ads_data: Dict) -> Dict[str, set]:
        """Извлекает уникальные ID из данных объявлений"""
        sitelink_set_ids = set()
        extension_ids = set()

        try:
            # Поддерживаем оба формата: новый (ads) и старый (result.Ads)
            if 'ads' in ads_data:
                ads = ads_data['ads']
            elif 'result' in ads_data and 'Ads' in ads_data['result']:
                ads = ads_data['result']['Ads']
            else:
                return {'sitelink_set_ids': sitelink_set_ids, 'extension_ids': extension_ids}

            print(f"📊 Обрабатываем {len(ads)} объявлений для извлечения ID")

            for ad in ads:
                # Извлекаем SitelinkSetId из TextAd
                if 'TextAd' in ad and 'SitelinkSetId' in ad['TextAd']:
                    sitelink_id = ad['TextAd']['SitelinkSetId']
                    if sitelink_id and sitelink_id not in sitelink_set_ids:
                        sitelink_set_ids.add(sitelink_id)
                        print(f"✅ Найден SitelinkSetId: {sitelink_id}")

                # Извлекаем AdExtensions из TextAd
                if 'TextAd' in ad and 'AdExtensions' in ad['TextAd']:
                    extensions = ad['TextAd']['AdExtensions']
                    for ext in extensions:
                        if 'AdExtensionId' in ext:
                            ext_id = ext['AdExtensionId']
                            if ext_id and ext_id not in extension_ids:
                                extension_ids.add(ext_id)
                                print(f"✅ Найден AdExtensionId: {ext_id}")

                # Извлекаем AdExtensions из DynamicTextAd
                if 'DynamicTextAd' in ad and 'AdExtensions' in ad['DynamicTextAd']:
                    extensions = ad['DynamicTextAd']['AdExtensions']
                    for ext in extensions:
                        if 'AdExtensionId' in ext:
                            ext_id = ext['AdExtensionId']
                            if ext_id and ext_id not in extension_ids:
                                extension_ids.add(ext_id)
                                print(f"✅ Найден AdExtensionId (DynamicTextAd): {ext_id}")

            print(f"\n📊 Итого найдено уникальных ID:")
            print(f"   SitelinkSetId: {len(sitelink_set_ids)}")
            print(f"   AdExtensionId: {len(extension_ids)}")

            return {
                'sitelink_set_ids': sitelink_set_ids,
                'extension_ids': extension_ids
            }

        except Exception as e:
            print(f"❌ Ошибка извлечения ID: {e}")
            return {'sitelink_set_ids': sitelink_set_ids, 'extension_ids': extension_ids}

    def extract_unique_image_hashes(self, ads_data: Dict) -> set:
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

    def save_ads_data(self, ads_data: Dict, report: Dict, request_data: Dict, contract_data: Dict):
        """Сохраняет данные объявлений в MinIO"""
        try:
            success = self.minio_client.upload_ads_data(ads_data, report['id'])
            if success:
                print(f"💾 Данные объявлений сохранены в MinIO для отчета {report['id']}")
            else:
                print(f"❌ Ошибка сохранения данных объявлений в MinIO")

        except Exception as e:
            print(f"❌ Ошибка сохранения данных объявлений: {e}")

    def save_sitelinks_data(self, sitelinks_data: Dict):
        """Сохраняет данные быстрых ссылок в MinIO"""
        try:
            success = self.minio_client.upload_sitelinks_data(sitelinks_data, self.current_report_id)
            if success:
                print(f"💾 Данные быстрых ссылок сохранены в MinIO для отчета {self.current_report_id}")
            else:
                print(f"❌ Ошибка сохранения данных быстрых ссылок в MinIO")

        except Exception as e:
            print(f"❌ Ошибка сохранения быстрых ссылок: {e}")

    def save_extensions_data(self, extensions_data: Dict):
        """Сохраняет данные расширений в MinIO"""
        try:
            success = self.minio_client.upload_extensions_data(extensions_data, self.current_report_id)
            if success:
                print(f"💾 Данные расширений сохранены в MinIO для отчета {self.current_report_id}")
            else:
                print(f"❌ Ошибка сохранения данных расширений в MinIO")

        except Exception as e:
            print(f"❌ Ошибка сохранения расширений: {e}")

    def save_image_data(self, image_data: Dict, unique_hashes: set, report_id: int):
        """Сохраняет данные изображений в MinIO"""
        try:
            success = self.minio_client.upload_image_data(image_data, report_id)
            if success:
                print(f"💾 Данные изображений сохранены в MinIO для отчета {report_id}")
            else:
                print(f"❌ Ошибка сохранения данных изображений в MinIO")

        except Exception as e:
            print(f"❌ Ошибка сохранения данных изображений: {e}")

    def save_keywords_data(self, keywords_data: Dict, report: Dict):
        """Сохраняет данные ключевых фраз в MinIO"""
        try:
            success = self.minio_client.upload_keywords_data(keywords_data, report['id'])
            if success:
                print(f"💾 Данные ключевых фраз сохранены в MinIO для отчета {report['id']}")
            else:
                print(f"❌ Ошибка сохранения данных ключевых фраз в MinIO")

        except Exception as e:
            print(f"❌ Ошибка сохранения данных ключевых фраз: {e}")

    def generate_report_urls(self, report: Dict, request_data: Dict, contract_data: Dict,
                             campaign_ids: List[int]) -> bool:
        """Генерирует URL отчетов"""
        try:
            # Получаем даты из заявки
            start_date, end_date = self.get_report_dates(request_data)
            if not start_date or not end_date:
                print("❌ Не найдены даты начала и окончания")
                return False

            # Получаем логин из договора
            login_yandex_direct = contract_data.get('login_yandex_direct')
            if not login_yandex_direct:
                print("❌ Не найден логин Яндекс.Директ в договоре")
                return False

            # Получаем удаленные группы из поля deleted_groups
            deleted_groups = self.get_deleted_groups(request_data)
            if deleted_groups:
                print(f"🚫 Найдено удаленных групп: {len(deleted_groups)}")
                print(f"🚫 ID удаленных групп: {deleted_groups}")

            # Создаем генератор URL
            url_generator = ReportURLGenerator()
            url_generator.db = self.db
            url_generator.minio_client = self.minio_client

            # Генерируем URL отчетов
            urls_data = url_generator.generate_report_urls(
                report, request_data, contract_data,
                campaign_ids, start_date, end_date, login_yandex_direct, deleted_groups
            )

            if urls_data:
                # Сохраняем данные в MinIO
                url_generator.save_urls_data(urls_data, report)
                return True
            else:
                print("❌ Не удалось сгенерировать URL отчетов")
                return False

        except Exception as e:
            print(f"❌ Ошибка генерации URL отчетов: {e}")
            return False

    def generate_screenshots(self, report: Dict) -> bool:
        """Генерирует скриншоты отчетов"""
        try:
            # Создаем генератор скриншотов
            screenshot_generator = ScreenshotGenerator()
            screenshot_generator.db = self.db
            screenshot_generator.minio_client = self.minio_client

            # Обрабатываем один отчет
            screenshot_generator.process_single_report(report)

            return True

        except Exception as e:
            print(f"❌ Ошибка генерации скриншотов: {e}")
            return False


def main():
    """Основная функция"""
    print("🚀 Запуск централизованной обработки всех скриптов")
    print("=" * 80)

    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY',
                     'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return

    while True:
        # Создаем и запускаем главный процессор
        processor = MainProcessor()

        try:
            success = processor.run_all_scripts()
            if success:
                print("\n✅ Все скрипты выполнены успешно")
            else:
                print("\n❌ Обработка завершена с ошибками")
        except Exception as e:
            print(f"\n❌ Критическая ошибка: {e}")
            raise e

        time.sleep(60)


if __name__ == "__main__":
    main()
