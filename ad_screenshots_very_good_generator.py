#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор скриншотов объявлений - скрипт для обработки отчетов из БД и загрузки данных из MinIO
"""

import os
import json
import psycopg2
from minio import Minio
from dotenv import load_dotenv
from typing import Dict, List, Optional, Any
from PIL import Image, ImageDraw, ImageFont
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import tempfile
import base64

# Загружаем переменные окружения
load_dotenv()


class AdScreenshotsGenerator:
    def __init__(self):
        """Инициализация подключений к БД и MinIO"""
        # Настройки БД
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

        # Настройки скриншотов
        self.ads_per_screenshot = 5  # Количество объявлений на одном скриншоте

        # Настройки MinIO
        self.s3_endpoint_url = os.getenv('S3_ENDPOINT_URL', 'minio.upk-mos.ru')
        self.s3_access_key = os.getenv('S3_ACCESS_KEY')
        self.s3_secret_key = os.getenv('S3_SECRET_KEY')
        self.s3_secure = os.getenv('S3_SECURE', 'False').lower() == 'true'
        self.s3_bucket_name = os.getenv('S3_BUCKET_NAME', 'dit-services-dev')

        self.minio_client = Minio(
            endpoint=self.s3_endpoint_url,
            access_key=self.s3_access_key,
            secret_key=self.s3_secret_key,
            secure=self.s3_secure
        )

        self.bucket_name = os.getenv('S3_BUCKET_NAME', 'dit-services-dev')

        # Инициализация веб-драйвера для HTML рендеринга
        self._setup_webdriver()

    def _setup_webdriver(self):
        """Настройка веб-драйвера для HTML рендеринга"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Запуск без GUI
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=300,600')
            chrome_options.add_argument('--force-device-scale-factor=1')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')

            # Автоматическая установка ChromeDriver
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            print("✓ Веб-драйвер инициализирован")
        except Exception as e:
            print(f"⚠ Ошибка инициализации веб-драйвера: {e}")
            self.driver = None

    def __del__(self):
        """Закрытие веб-драйвера при удалении объекта"""
        if hasattr(self, 'driver') and self.driver:
            try:
                self.driver.quit()
            except:
                pass

    def get_pending_reports(self) -> List[Dict]:
        """Получить отчеты со статусом 1 (готовые к обработке)"""
        try:
            print(f"🔌 Подключение к БД: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            # Устанавливаем схему по умолчанию
            cursor.execute("SET search_path TO gen_report_context_contracts, public;")
            print("✓ Схема установлена: gen_report_context_contracts")

            # Сначала проверим, что таблицы существуют
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'gen_report_context_contracts' 
                AND table_name IN ('reports', 'contracts')
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            print(f"📋 Найденные таблицы в схеме: {[table[0] for table in tables]}")

            # Проверим общее количество отчетов
            cursor.execute("SELECT COUNT(*) FROM reports")
            total_reports = cursor.fetchone()[0]
            print(f"📊 Всего отчетов в БД: {total_reports}")

            # Проверим отчеты со статусом 1
            cursor.execute("SELECT COUNT(*) FROM reports WHERE id_status = 1")
            status_1_reports = cursor.fetchone()[0]
            print(f"📋 Отчетов со статусом 1: {status_1_reports}")

            # Проверим отчеты со статусом 1 и не удаленные (включая NULL)
            cursor.execute(
                "SELECT COUNT(*) FROM reports WHERE id_status = 1 AND (is_deleted = false OR is_deleted IS NULL)")
            pending_reports = cursor.fetchone()[0]
            print(f"📋 Отчетов со статусом 1 и не удаленных (включая NULL): {pending_reports}")

            # Покажем все статусы отчетов
            cursor.execute("""
                SELECT id_status, COUNT(*) as count 
                FROM reports 
                GROUP BY id_status 
                ORDER BY id_status;
            """)
            status_counts = cursor.fetchall()
            print(f"📊 Статистика по статусам: {dict(status_counts)}")

            # Покажем детали отчета со статусом 1
            cursor.execute("""
                SELECT id, id_contracts, id_requests, id_status, is_deleted, create_entry, message
                FROM reports 
                WHERE id_status = 1
                ORDER BY create_entry DESC;
            """)
            status_1_details = cursor.fetchall()
            print(f"🔍 Детали отчетов со статусом 1:")
            for report in status_1_details:
                print(
                    f"  ID: {report[0]}, Договор: {report[1]}, Заявка: {report[2]}, Статус: {report[3]}, Удален: {report[4]}, Создан: {report[5]}, Сообщение: {report[6]}")

            if pending_reports == 0:
                print("⚠ Нет отчетов со статусом 1 для обработки")
                print("💡 Возможно, отчеты помечены как удаленные (is_deleted = true)")
                cursor.close()
                conn.close()
                return []

            query = """
            SELECT r.id, r.id_contracts, r.id_requests, c.number_contract, c.subject_contract
            FROM reports r
            JOIN contracts c ON r.id_contracts = c.id
            WHERE r.id_status = 1 AND (r.is_deleted = false OR r.is_deleted IS NULL)
            ORDER BY r.create_entry DESC
            """

            print(f"🔍 Выполняем запрос: {query}")
            cursor.execute(query)
            reports = []

            for row in cursor.fetchall():
                reports.append({
                    'id': row[0],
                    'id_contracts': row[1],
                    'id_requests': row[2],
                    'number_contract': row[3],
                    'subject_contract': row[4]
                })

            print(f"✅ Найдено отчетов для обработки: {len(reports)}")

            cursor.close()
            conn.close()

            return reports

        except Exception as e:
            print(f"Ошибка при получении отчетов из БД: {e}")
            return []

    def load_data_from_minio(self, report_id: int) -> Dict[str, Any]:
        """Загрузить данные из MinIO для конкретного отчета"""
        data = {}

        # Путь к папке с данными отчета
        folder_path = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты"

        # Список файлов для загрузки (используем номер отчета)
        files_to_load = [
            f'ads_report_{report_id}.json',
            f'extensions_{report_id}.json',
            f'image_hashes_report_{report_id}.json',
            f'keywords_traffic_forecast_{report_id}.json',
            f'sitelinks_{report_id}.json',
            f'ad_stats_{report_id}.json'  # Добавляем файл статистики
        ]

        for filename in files_to_load:
            try:
                object_path = f"{folder_path}/{filename}"

                # Проверяем существование объекта
                if self.minio_client.stat_object(self.bucket_name, object_path):
                    # Загружаем объект
                    response = self.minio_client.get_object(self.bucket_name, object_path)
                    content = response.read().decode('utf-8')
                    data[filename] = json.loads(content)
                    print(f"✓ Загружен файл: {filename}")
                else:
                    print(f"⚠ Файл не найден: {filename}")

            except Exception as e:
                print(f"✗ Ошибка при загрузке {filename}: {e}")
                data[filename] = None

        return data

    def get_top_ads_by_clicks(self, ad_stats_data: Dict, top_count: int = 10) -> List[Dict]:
        """Получить топ объявлений по количеству кликов с учетом BounceRate"""
        if not ad_stats_data or 'result' not in ad_stats_data:
            print("⚠ Нет данных статистики объявлений")
            return []

        rows = ad_stats_data['result'].get('rows', [])
        if not rows:
            print("⚠ Нет строк в данных статистики")
            return []

        print(f"📊 Всего объявлений в статистике: {len(rows)}")

        # Применяем фильтрацию по BounceRate с адаптивной логикой
        top_ads = self._filter_by_bounce_rate_and_sort(rows, top_count)

        print(f"🏆 Топ {len(top_ads)} объявлений по кликам (после фильтрации по отказу):")
        for i, ad in enumerate(top_ads, 1):
            ad_id = ad.get('AdId')
            clicks = ad.get('Clicks', 0)
            impressions = ad.get('Impressions', 0)
            ctr = ad.get('Ctr', 0)
            bounce_rate = ad.get('BounceRate', 0)
            cost = ad.get('Cost', 0)
            print(
                f"  {i}. ID: {ad_id}, Клики: {clicks}, Показы: {impressions}, CTR: {ctr}%, Отказы: {bounce_rate}%, Стоимость: {cost}")

        return top_ads

    def _filter_by_bounce_rate_and_sort(self, rows: List[Dict], top_count: int, initial_threshold: float = 35.0) -> \
            List[Dict]:
        """Фильтрация по BounceRate с адаптивной логикой"""
        threshold = initial_threshold

        print(f"\n🔍 ФИЛЬТРАЦИЯ ПО BounceRate (начальный порог: {threshold}%):")

        # Первая попытка: фильтруем по 35% отказов
        filtered = [ad for ad in rows if ad.get('BounceRate', 0) <= threshold]
        print(f"  ✓ Объявлений с BounceRate <= {threshold}%: {len(filtered)}")

        # Если после фильтрации осталось 0 или 1 объявление, увеличиваем порог до 50%
        if len(filtered) <= 1:
            print(f"  ⚠ Слишком мало объявлений ({len(filtered)}), увеличиваем порог до 50%")
            threshold = 50.0
            filtered = [ad for ad in rows if ad.get('BounceRate', 0) <= threshold]
            print(f"  ✓ Объявлений с BounceRate <= {threshold}%: {len(filtered)}")

        # Если и после увеличения порога осталось 0 или 1 объявление, убираем ограничение
        if len(filtered) <= 1:
            print(f"  ⚠ Все еще слишком мало объявлений ({len(filtered)}), убираем ограничение по BounceRate")
            filtered = rows
            print(f"  ✓ Объявлений без ограничения по BounceRate: {len(filtered)}")

        # Сортируем по количеству кликов (по убыванию)
        sorted_ads = sorted(filtered, key=lambda x: x.get('Clicks', 0), reverse=True)

        # Берем топ N объявлений
        top_ads = sorted_ads[:top_count]

        return top_ads

    def load_ad_details_from_stats(self, top_ads: List[Dict], report_id: int) -> List[Dict]:
        """Загрузить детали объявлений из ads_report для топ объявлений"""
        # Загружаем данные объявлений
        data = self.load_data_from_minio(report_id)
        ads_report_key = f'ads_report_{report_id}.json'

        if not data.get(ads_report_key):
            print("⚠ Нет данных объявлений")
            return []

        ads_data = data[ads_report_key]
        all_ads = ads_data.get('result', {}).get('Ads', [])

        # Создаем словарь для быстрого поиска по ID
        ads_dict = {ad.get('Id'): ad for ad in all_ads}

        # Собираем детали для топ объявлений
        top_ads_details = []
        for ad_stat in top_ads:
            ad_id = ad_stat.get('AdId')
            if ad_id in ads_dict:
                ad_detail = ads_dict[ad_id]
                # Добавляем статистику к деталям объявления
                ad_detail['statistics'] = ad_stat
                top_ads_details.append(ad_detail)
                print(f"✓ Найдены детали для объявления ID: {ad_id}")
            else:
                print(f"⚠ Не найдены детали для объявления ID: {ad_id}")

        return top_ads_details

    def generate_top_ads_screenshots(self, top_ads_details: List[Dict], report_id: int,
                                     output_dir: str = "screenshots") -> List[str]:
        """Создать скриншоты для топ объявлений (по одному объявлению на скриншот)"""
        created_screenshots = []

        if not top_ads_details:
            print("⚠ Нет объявлений для создания скриншотов")
            return created_screenshots

        # Загружаем дополнительные данные
        data = self.load_data_from_minio(report_id)
        sitelinks_data = data.get(f'sitelinks_{report_id}.json')
        extensions_data = data.get(f'extensions_{report_id}.json')
        image_data = data.get(f'image_hashes_report_{report_id}.json')

        print(f"\n🖼️ ГЕНЕРАЦИЯ СКРИНШОТОВ ДЛЯ ТОП {len(top_ads_details)} ОБЪЯВЛЕНИЙ:")
        print(f"📊 Создаем по 1 объявлению на скриншот")

        for i, ad_detail in enumerate(top_ads_details, 1):
            ad_id = ad_detail.get('Id')
            statistics = ad_detail.get('statistics', {})
            clicks = statistics.get('Clicks', 0)

            print(f"\n📸 Создание скриншота #{i} для объявления ID: {ad_id} (клики: {clicks}):")

            # Получаем sitelinks для объявления
            sitelinks = self._get_sitelinks_for_ad(ad_detail, sitelinks_data)

            # Получаем изображение для объявления
            image_url = self._get_image_for_ad(ad_detail, image_data)

            # Создаем скриншот (используем ID объявления как имя файла)
            screenshot_path = self.generate_single_ad_screenshot(
                ad_detail,
                ad_id,  # Используем ID объявления вместо индекса
                output_dir,
                sitelinks_data,
                extensions_data,
                image_data,
                report_id
            )

            if screenshot_path:
                created_screenshots.append(screenshot_path)
                print(f"  ✅ Скриншот для ID {ad_id} сохранен: {screenshot_path}")
            else:
                print(f"  ❌ Ошибка создания скриншота для ID {ad_id}")

        print(f"\n✅ Создано скриншотов: {len(created_screenshots)} из {len(top_ads_details)}")
        return created_screenshots

    def generate_single_ad_screenshot(self, ad_data: Dict, ad_id: str, output_dir: str = "screenshots",
                                      sitelinks_data: Dict = None, extensions_data: Dict = None,
                                      image_data: Dict = None, report_id: int = None) -> str:
        """Генерировать скриншот одного объявления с именем файла по ID"""
        try:
            # Создаем папку для скриншотов, если её нет
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Получаем данные объявления
            text_ad = ad_data.get('TextAd', {})

            if text_ad:
                title = text_ad.get('Title', 'Заголовок не найден')
                title2 = text_ad.get('Title2', 'Подзаголовок не найден')
                # Убеждаемся, что текст в правильной кодировке
                title = str(title).encode('utf-8').decode('utf-8')
                title2 = str(title2).encode('utf-8').decode('utf-8')
                display_text = f"{title} - {title2}"
            else:
                # Если нет TextAd, используем ID и тип объявления
                ad_type = ad_data.get('Type', 'Неизвестный тип')
                display_text = f"Объявление ID: {ad_id} - Тип: {ad_type}"

            # Проверяем доступность веб-драйвера
            if not self.driver:
                print("⚠ Веб-драйвер недоступен, используем fallback метод")
                return self._generate_fallback_single_screenshot(ad_data, ad_id, output_dir)

            # Получаем sitelinks для объявления
            sitelinks = self._get_sitelinks_for_ad(ad_data, sitelinks_data)

            # Получаем изображение для объявления
            image_url = self._get_image_for_ad(ad_data, image_data)

            # Создаем HTML с правильным форматированием
            html_content = self._create_html_content(display_text, sitelinks, ad_data, extensions_data, image_url)

            # Создаем временный HTML файл
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                f.write(html_content)
                temp_html_path = f.name

            try:
                # Загружаем HTML в браузер
                self.driver.get(f"file://{temp_html_path}")

                # Ждем загрузки контента
                self.driver.implicitly_wait(2)

                # Сначала устанавливаем базовый размер для измерения
                self.driver.set_window_size(300, 400)

                # Ждем загрузки изображения (если есть)
                if image_url:
                    print(f"      🖼️ Ждем загрузки изображения: {image_url}")
                    self.driver.implicitly_wait(3)

                # Получаем размеры основного контента
                ad_element = self.driver.find_element("id", "ad-content")
                ad_size = ad_element.size

                # Проверяем, есть ли sitelinks
                sitelinks_element = None
                sitelinks_size = {'height': 0}
                try:
                    sitelinks_element = self.driver.find_element("id", "sitelinks")
                    sitelinks_size = sitelinks_element.size
                except:
                    pass  # Sitelinks нет

                # Проверяем, есть ли URL
                url_element = None
                url_size = {'height': 0}
                try:
                    url_element = self.driver.find_element("id", "url")
                    url_size = url_element.size
                except:
                    pass  # URL нет

                # Проверяем, есть ли текст объявления
                text_element = None
                text_size = {'height': 0}
                try:
                    text_element = self.driver.find_element("id", "ad-text")
                    text_size = text_element.size
                except:
                    pass  # Текст нет

                # Проверяем, есть ли расширения
                extensions_element = None
                extensions_size = {'height': 0}
                try:
                    extensions_element = self.driver.find_element("id", "extensions")
                    extensions_size = extensions_element.size
                except:
                    pass  # Расширения нет

                # Рассчитываем общую высоту контента
                content_height = (ad_size['height'] +
                                  sitelinks_size['height'] +
                                  url_size['height'] +
                                  text_size['height'] +
                                  extensions_size['height'] + 20)  # Отступы

                # Высота изображения фиксированная
                image_height = 200 if image_url else 0

                # Общая высота = высота изображения + высота контента (увеличиваем в 3 раза)
                total_height = int((image_height + content_height) * 3)

                print(f"      📏 Размеры элементов:")
                print(f"        - ad-content: {ad_size['height']}px")
                print(f"        - sitelinks: {sitelinks_size['height']}px")
                print(f"        - url: {url_size['height']}px")
                print(f"        - ad-text: {text_size['height']}px")
                print(f"        - extensions: {extensions_size['height']}px")
                print(f"        - content total: {content_height}px")
                print(f"        - image height: {image_height}px")
                print(f"        - total (x3): {total_height}px")

                # Устанавливаем точную высоту окна
                self.driver.set_window_size(300, total_height)

                # Принудительно устанавливаем размер viewport
                self.driver.execute_script(f"""
                    document.body.style.width='300px'; 
                    document.body.style.margin='0'; 
                    document.body.style.padding='0';
                """)

                # Ждем финальной загрузки
                self.driver.implicitly_wait(1)

                # Получаем финальные размеры body
                body_element = self.driver.find_element("tag name", "body")
                final_body_size = body_element.size

                print(f"      📏 Финальные размеры body: {final_body_size}")

                # Делаем скриншот всего body
                screenshot = body_element.screenshot_as_png

                # Сохраняем файл с именем по ID объявления
                filename = f"{ad_id}.png"
                filepath = os.path.join(output_dir, filename)

                with open(filepath, 'wb') as f:
                    f.write(screenshot)

                print(f"✓ Создан HTML скриншот: {filename} (размер: {total_height}px)")

                # Загружаем в MinIO
                if report_id:
                    minio_path = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/very_good_ads/{filename}"
                else:
                    minio_path = f"gen_report_context_contracts/data_yandex_direct/unknown_результаты/very_good_ads/{filename}"
                if self.upload_to_minio(filepath, minio_path):
                    # Удаляем локальный файл после успешной загрузки
                    try:
                        os.remove(filepath)
                        print(f"      🗑️ Локальный файл удален: {filename}")
                    except:
                        pass
                    return minio_path
                else:
                    return filepath

            finally:
                # Удаляем временный файл
                try:
                    os.unlink(temp_html_path)
                except:
                    pass

        except Exception as e:
            print(f"✗ Ошибка при создании HTML скриншота: {e}")
            # Fallback к старому методу
            return self._generate_fallback_single_screenshot(ad_data, ad_id, output_dir, report_id)

    def _generate_fallback_single_screenshot(self, ad_data: Dict, ad_id: str, output_dir: str,
                                             report_id: int = None) -> str:
        """Fallback метод генерации скриншота с использованием PIL (для одного объявления)"""
        try:
            # Пытаемся получить данные из TextAd
            text_ad = ad_data.get('TextAd', {})

            if text_ad:
                title = text_ad.get('Title', 'Заголовок не найден')
                title2 = text_ad.get('Title2', 'Подзаголовок не найден')
                title = str(title).encode('utf-8').decode('utf-8')
                title2 = str(title2).encode('utf-8').decode('utf-8')
                display_text = f"{title} - {title2}"
            else:
                ad_type = ad_data.get('Type', 'Неизвестный тип')
                display_text = f"Объявление ID: {ad_id} - Тип: {ad_type}"

            # Размеры изображения (fallback метод) - узкий и высокий
            width = 300
            height = 600

            # Создаем изображение с белым фоном
            img = Image.new('RGB', (width, height), 'white')
            draw = ImageDraw.Draw(img)

            # Пытаемся загрузить жирный шрифт
            try:
                font_paths = [
                    "C:/Windows/Fonts/arialbd.ttf",
                    "C:/Windows/Fonts/calibrib.ttf",
                    "C:/Windows/Fonts/tahomabd.ttf",
                    "C:/Windows/Fonts/arial.ttf",
                    "/System/Library/Fonts/Arial Bold.ttf",
                    "/System/Library/Fonts/Arial.ttf"
                ]

                font = None
                for font_path in font_paths:
                    if os.path.exists(font_path):
                        try:
                            font = ImageFont.truetype(font_path, 14)
                            break
                        except:
                            continue

                if font is None:
                    font = ImageFont.load_default()

            except Exception as e:
                font = ImageFont.load_default()

            # Цвет текста #000080 (темно-синий)
            text_color = (0, 0, 128)

            # Позиционируем текст
            x = 20
            y = 20

            # Рисуем текст
            draw.text((x, y), display_text, fill=text_color, font=font)

            # Сохраняем файл с именем по ID объявления
            filename = f"{ad_id}.png"
            filepath = os.path.join(output_dir, filename)
            img.save(filepath)

            print(f"✓ Создан fallback скриншот: {filename}")

            # Загружаем в MinIO
            if report_id:
                minio_path = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/very_good_ads/{filename}"
            else:
                minio_path = f"gen_report_context_contracts/data_yandex_direct/unknown_результаты/very_good_ads/{filename}"

            if self.upload_to_minio(filepath, minio_path):
                # Удаляем локальный файл после успешной загрузки
                try:
                    os.remove(filepath)
                    print(f"      🗑️ Локальный файл удален: {filename}")
                except:
                    pass
                return minio_path
            else:
                return filepath

        except Exception as e:
            print(f"✗ Ошибка при создании fallback скриншота: {e}")
            return None

    def _get_image_for_ad(self, ad_data: Dict, image_data: Dict) -> str:
        """Получить URL изображения для объявления"""
        if not image_data:
            print(f"      ⚠ Нет данных изображений")
            return None

        # Получаем AdImageHash из объявления
        text_ad = ad_data.get('TextAd', {})
        ad_image_hash = text_ad.get('AdImageHash')

        if not ad_image_hash:
            print(f"      ⚠ Нет AdImageHash в объявлении")
            return None

        print(f"      🔍 Ищем изображение с хешем: {ad_image_hash}")

        # Получаем все изображения
        ad_images = image_data.get('result', {}).get('AdImages', [])
        print(f"      📸 Всего изображений в данных: {len(ad_images)}")

        # Ищем изображение по AdImageHash
        for image in ad_images:
            image_hash = image.get('AdImageHash')
            if image_hash == ad_image_hash:
                original_url = image.get('OriginalUrl')
                if original_url:
                    print(f"      ✅ Найдено изображение: {original_url}")
                    return original_url
                else:
                    print(f"      ⚠ Нет OriginalUrl в найденном изображении")
                    return None

        print(f"      ⚠ Изображение с хешем {ad_image_hash} не найдено")
        return None

    def _get_sitelinks_for_ad(self, ad_data: Dict, sitelinks_data: Dict) -> List[Dict]:
        """Получить sitelinks для конкретного объявления"""
        if not sitelinks_data:
            print(f"      ⚠ Нет данных sitelinks")
            return []

        # Получаем SitelinkSetId из объявления
        text_ad = ad_data.get('TextAd', {})
        sitelink_set_id = text_ad.get('SitelinkSetId')

        print(f"      🔍 SitelinkSetId из объявления: {sitelink_set_id}")

        if not sitelink_set_id:
            print(f"      ⚠ У объявления нет SitelinkSetId")
            return []

        # Ищем sitelinks по ID - проверяем структуру данных
        print(f"      🔍 Структура sitelinks_data: {list(sitelinks_data.keys())}")

        # Пробуем найти sitelinks в разных структурах
        sitelinks_sets = []

        # Вариант 1: прямая структура
        if 'result' in sitelinks_data:
            sitelinks_sets = sitelinks_data.get('result', {}).get('SitelinksSets', [])
            print(f"      📋 Найдено наборов sitelinks (вариант 1): {len(sitelinks_sets)}")

        # Вариант 2: структура с ID как ключ
        if str(sitelink_set_id) in sitelinks_data:
            sitelinks_data_by_id = sitelinks_data[str(sitelink_set_id)]
            sitelinks_sets = sitelinks_data_by_id.get('result', {}).get('SitelinksSets', [])
            print(f"      📋 Найдено наборов sitelinks (вариант 2): {len(sitelinks_sets)}")

        for sitelinks_set in sitelinks_sets:
            set_id = sitelinks_set.get('Id')
            print(f"      🔍 Проверяем набор с ID: {set_id}")
            if set_id == sitelink_set_id:
                # Берем первые 4 sitelinks
                sitelinks = sitelinks_set.get('Sitelinks', [])
                print(f"      ✅ Найдены sitelinks: {len(sitelinks)} штук")
                for i, sitelink in enumerate(sitelinks[:4]):
                    print(f"        {i + 1}. {sitelink.get('Title', 'Без заголовка')}")
                return sitelinks[:4]

        print(f"      ❌ Sitelinks с ID {sitelink_set_id} не найдены")
        return []

    def _create_html_content(self, display_text: str, sitelinks: List[Dict] = None, ad_data: Dict = None,
                             extensions_data: Dict = None, image_url: str = None) -> str:
        """Создание HTML контента для скриншота"""
        # Экранируем HTML символы
        safe_text = display_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

        # Формируем sitelinks HTML
        sitelinks_html = ""
        print(f"      🔍 Обрабатываем sitelinks: {len(sitelinks) if sitelinks else 0} штук")

        if sitelinks:
            sitelinks_titles = []
            for sitelink in sitelinks:
                title = sitelink.get('Title', '')
                if title:
                    # Экранируем HTML символы в заголовке
                    safe_title = title.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"',
                                                                                                               '&quot;')
                    sitelinks_titles.append(safe_title)
                    print(f"        📝 Добавлен sitelink: {safe_title}")

            if sitelinks_titles:
                # Объединяем заголовки с 5 пробелами между ними
                sitelinks_text = "     ".join(sitelinks_titles)
                sitelinks_html = f'<div id="sitelinks">{sitelinks_text}</div>'
                print(f"      ✅ Сформирован HTML для sitelinks: {sitelinks_text[:50]}...")
            else:
                print(f"      ⚠ Нет заголовков для sitelinks")
        else:
            print(f"      ⚠ Sitelinks пустые или None")

        # Формируем URL строку
        url_html = ""
        if ad_data:
            text_ad = ad_data.get('TextAd', {})
            href = text_ad.get('Href', '')
            display_url_path = text_ad.get('DisplayUrlPath', '')

            if href and display_url_path:
                # Экранируем HTML символы
                safe_href = href.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')
                safe_display_url = display_url_path.replace('&', '&amp;').replace('<', '&lt;').replace('>',
                                                                                                       '&gt;').replace(
                    '"', '&quot;')
                url_text = f"{safe_href} > {safe_display_url}"
                url_html = f'<div id="url">{url_text}</div>'
                print(f"      ✅ Сформирован URL: {url_text}")
            else:
                print(f"      ⚠ Нет данных для URL (href: {href}, display_url: {display_url_path})")

        # Формируем текст объявления
        text_html = ""
        if ad_data:
            text_ad = ad_data.get('TextAd', {})
            text_content = text_ad.get('Text', '')

            if text_content:
                # Экранируем HTML символы
                safe_text_content = text_content.replace('&', '&amp;').replace('<', '&lt;').replace('>',
                                                                                                    '&gt;').replace('"',
                                                                                                                    '&quot;')
                text_html = f'<div id="ad-text">{safe_text_content}</div>'
                print(f"      ✅ Сформирован текст объявления: {safe_text_content[:50]}...")
            else:
                print(f"      ⚠ Нет текста объявления")

        # Формируем HTML для изображения
        image_html = ""
        if image_url:
            image_html = f'<div class="ad-image"><img src="{image_url}" alt="Ad Image"></div>'
            print(f"      ✅ Добавлено изображение: {image_url}")
        else:
            print(f"      ⚠ Изображение не найдено")

        # Формируем расширения (extensions)
        extensions_html = ""
        if ad_data and extensions_data:
            # Получаем AdExtensionId из объявления
            text_ad = ad_data.get('TextAd', {})
            ad_extensions = text_ad.get('AdExtensions', [])

            if ad_extensions:
                extension_ids = [ext.get('AdExtensionId') for ext in ad_extensions if ext.get('AdExtensionId')]
                print(f"      🔍 Найдены AdExtensionId: {extension_ids}")

                # Получаем все расширения из extensions_data
                all_extensions = extensions_data.get('batch_1', {}).get('result', {}).get('AdExtensions', [])
                print(f"      📋 Всего расширений в данных: {len(all_extensions)}")

                # Фильтруем расширения по ID
                matching_extensions = []
                for ext in all_extensions:
                    if ext.get('Id') in extension_ids:
                        callout = ext.get('Callout', {})
                        callout_text = callout.get('CalloutText', '')
                        if callout_text:
                            matching_extensions.append(callout_text)
                            print(f"        📝 Найдено расширение: {callout_text}")

                if matching_extensions:
                    # Объединяем через символ "·" с пробелами
                    extensions_text = " · ".join(matching_extensions)
                    extensions_html = f'<div id="extensions">{extensions_text}</div>'
                    print(f"      ✅ Сформированы расширения: {extensions_text}")
                else:
                    print(f"      ⚠ Не найдены соответствующие расширения")
            else:
                print(f"      ⚠ Нет AdExtensions в объявлении")
        else:
            print(f"      ⚠ Нет данных для расширений")

        html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ad Screenshot</title>
    <style>
        body {{
            margin: 0;
            padding: 0;
            font-family: 'Arial', 'Helvetica', sans-serif;
            background-color: white;
            overflow: hidden;
        }}
        
        #ad-content {{
            width: 100%;
            padding: 10px;
            background-color: white;
            color: #534fd8;
            font-size: 16px;
            font-weight: bold;
            line-height: 1.4;
            word-wrap: break-word;
            overflow-wrap: break-word;
            hyphens: auto;
            white-space: pre-wrap;
            box-sizing: border-box;
            margin: 0;
            border: none;
            text-align: left;
        }}
        
        #sitelinks {{
            width: 100%;
            padding: 5px 10px;
            background-color: white;
            color: #6d6493;
            font-size: 12px;
            font-weight: normal;
            line-height: 1.3;
            word-wrap: break-word;
            overflow-wrap: break-word;
            white-space: pre-wrap;
            box-sizing: border-box;
            margin: 0;
            border: none;
            text-align: left;
        }}
        
        #url {{
            width: 100%;
            padding: 5px 10px;
            background-color: white;
            color: #4b8e4b;
            font-size: 12px;
            font-weight: normal;
            line-height: 1.3;
            word-wrap: break-word;
            overflow-wrap: break-word;
            white-space: pre-wrap;
            box-sizing: border-box;
            margin: 0;
            border: none;
            text-align: left;
        }}
        
        #ad-text {{
            width: 100%;
            padding: 5px 10px;
            background-color: white;
            color: #000000;
            font-size: 12px;
            font-weight: normal;
            line-height: 1.3;
            word-wrap: break-word;
            overflow-wrap: break-word;
            white-space: pre-wrap;
            box-sizing: border-box;
            margin: 0;
            border: none;
            text-align: left;
        }}
        
        #extensions {{
            width: 100%;
            padding: 5px 10px 10px 10px;
            background-color: white;
            color: #000000;
            font-size: 12px;
            font-weight: normal;
            line-height: 1.3;
            word-wrap: break-word;
            overflow-wrap: break-word;
            white-space: pre-wrap;
            box-sizing: border-box;
            margin: 0;
            border: none;
            text-align: left;
        }}
        
        body {{
            margin: 0;
            padding: 0;
            width: 300px;
            overflow-x: hidden;
        }}
        
        .ad-container {{
            display: flex;
            flex-direction: column;
            width: 300px;
            background-color: white;
            margin: 0;
            padding: 0;
        }}
        
        .ad-image {{
            width: 300px;
            height: 200px;
            flex-shrink: 0;
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: white;
            padding: 10px;
            box-sizing: border-box;
            overflow: hidden;
        }}
        
        .ad-image img {{
            max-width: 100%;
            max-height: 100%;
            width: auto;
            height: auto;
            object-fit: contain;
            object-position: center;
        }}
        
        .ad-content-wrapper {{
            width: 100%;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            align-items: flex-start;
        }}
        
        /* Поддержка жирного шрифта */
        @font-face {{
            font-family: 'Arial Bold';
            src: local('Arial Bold'), local('Arial-Bold');
            font-weight: bold;
        }}
        
        #ad-content {{
            font-family: 'Arial Bold', 'Arial', sans-serif;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="ad-container">
        {image_html}
        <div class="ad-content-wrapper">
            <div id="ad-content">{safe_text}</div>
            {sitelinks_html}
            {url_html}
            {text_html}
            {extensions_html}
        </div>
    </div>
</body>
</html>
        """
        return html_content

    def upload_to_minio(self, file_path: str, minio_path: str) -> bool:
        """Загрузить файл в MinIO"""
        try:
            # Загружаем файл используя уже существующий клиент
            self.minio_client.fput_object(
                self.s3_bucket_name,
                minio_path,
                file_path
            )

            print(f"      ✅ Загружено в MinIO: {minio_path}")
            return True

        except Exception as e:
            print(f"      ❌ Ошибка загрузки в MinIO: {e}")
            return False

    def process_top_ads_report(self, report: Dict) -> None:
        """Обработать отчет для топ объявлений по кликам"""
        print(f"\n{'=' * 60}")
        print(f"ОБРАБОТКА ТОП ОБЪЯВЛЕНИЙ ОТЧЕТА #{report['id']}")
        print(f"{'=' * 60}")
        print(f"ID отчета: {report['id']}")
        print(f"ID договора: {report['id_contracts']}")
        print(f"Номер договора: {report['number_contract']}")
        print(f"Предмет договора: {report['subject_contract']}")

        # Загружаем данные из MinIO
        print(f"\nЗагрузка данных из MinIO...")
        data = self.load_data_from_minio(report['id'])

        # Получаем данные статистики
        ad_stats_key = f'ad_stats_{report["id"]}.json'
        ad_stats_data = data.get(ad_stats_key)

        if not ad_stats_data:
            print(f"⚠ Нет данных статистики объявлений в файле {ad_stats_key}")
            return

        # Получаем топ 10 объявлений по кликам
        print(f"\n📊 АНАЛИЗ СТАТИСТИКИ ОБЪЯВЛЕНИЙ:")
        top_ads = self.get_top_ads_by_clicks(ad_stats_data, top_count=10)

        if not top_ads:
            print("⚠ Не удалось получить топ объявления")
            return

        # Загружаем детали объявлений
        print(f"\n🔍 ЗАГРУЗКА ДЕТАЛЕЙ ОБЪЯВЛЕНИЙ:")
        top_ads_details = self.load_ad_details_from_stats(top_ads, report['id'])

        if not top_ads_details:
            print("⚠ Не удалось загрузить детали объявлений")
            return

        # Создаем скриншоты для топ объявлений
        print(f"\n🖼️ СОЗДАНИЕ СКРИНШОТОВ ДЛЯ ТОП ОБЪЯВЛЕНИЙ:")
        created_screenshots = self.generate_top_ads_screenshots(top_ads_details, report['id'], ".")

        print(f"\n✅ ОБРАБОТКА ЗАВЕРШЕНА:")
        print(f"📊 Обработано объявлений: {len(top_ads_details)}")
        print(f"🖼️ Создано скриншотов: {len(created_screenshots)}")

        # Выводим список созданных файлов
        if created_screenshots:
            print(f"\n📁 СОЗДАННЫЕ СКРИНШОТЫ:")
            for screenshot_path in created_screenshots:
                filename = os.path.basename(screenshot_path)
                print(f"  - {filename}")

    def run(self):
        """Основной метод запуска обработки топ объявлений по кликам"""
        print("🚀 Запуск генератора скриншотов для топ объявлений по кликам...")

        # Получаем отчеты для обработки
        reports = self.get_pending_reports()

        if not reports:
            print("📭 Нет отчетов со статусом 1 для обработки")
            return

        print(f"📋 Найдено отчетов для обработки: {len(reports)}")

        # Обрабатываем каждый отчет для топ объявлений
        for report in reports:
            try:
                self.process_top_ads_report(report)
            except Exception as e:
                print(f"❌ Ошибка при обработке топ объявлений отчета {report['id']}: {e}")
                continue

        print(f"\n✅ Обработка топ объявлений завершена. Обработано отчетов: {len(reports)}")

    def single_run(self, report_id):
        """Метод запуска обработки топ объявлений по кликам для одного отчёта"""
        print("🚀 Запуск генератора скриншотов для топ объявлений по кликам...")

        try:
            print(f"🔌 Подключение к БД: {self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            schema = os.getenv('DB_SCHEMA')

            query = f"""
                SELECT r.id, r.id_contracts, r.id_requests, c.number_contract, c.subject_contract
                FROM {schema}.reports r
                JOIN {schema}.contracts c ON r.id_contracts = c.id
                WHERE r.id = {report_id} AND (r.is_deleted = false OR r.is_deleted IS NULL)
                """

            cursor.execute(query)
            report = cursor.fetchall()
            if report:
                report = report[0]
                report = {
                    'id': report[0],
                    'id_contracts': report[1],
                    'id_requests': report[2],
                    'number_contract': report[3],
                    'subject_contract': report[4]}
                self.process_top_ads_report(report)
            else:
                raise ValueError(f'Отчёт {report_id} не найден')

        except Exception as e:
            print(f"❌ Ошибка при обработке топ объявлений отчета {report_id}: {e}")
            raise
        finally:
            conn.close()


def very_good_screenshot_generator(report_id):
    """Главная функция"""
    try:
        generator = AdScreenshotsGenerator()
        generator.single_run(report_id)

        # report = get_report_by_id(report_id)

        # generator.process_top_ads_report(report)

        # generator.run()

    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    # very_good_screenshot_generator()
    print(very_good_screenshot_generator('19'))
