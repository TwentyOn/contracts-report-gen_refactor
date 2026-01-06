#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Генератор скриншотов объявлений - скрипт для обработки отчетов из БД и загрузки данных из MinIO
"""
import io
import os
import json
import traceback
from zipfile import ZipFile

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


from generate_report_files.screen_ads.postprocess import create_and_packaging_zip, html_remove

# Загружаем переменные окружения
load_dotenv('.env')


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
        self.minio_client = Minio(
            endpoint=os.getenv('S3_ENDPOINT_URL', 'minio.upk-mos.ru'),
            access_key=os.getenv('S3_ACCESS_KEY'),
            secret_key=os.getenv('S3_SECRET_KEY'),
            secure=os.getenv('S3_SECURE', 'False').lower() == 'true'
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
            chrome_options.add_argument('--window-size=1000,300')
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
            f'sitelinks_{report_id}.json'
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

    def generate_multi_ad_screenshot(self, ads_data: List[Dict], screenshot_index: int,
                                     output_dir: str = "screenshots") -> str:
        """Генерировать скриншот с несколькими объявлениями"""
        try:
            # Создаем папку для скриншотов, если её нет
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Проверяем доступность веб-драйвера
            if not self.driver:
                print("⚠ Веб-драйвер недоступен, используем fallback метод")
                return self._generate_fallback_multi_screenshot(ads_data, screenshot_index, output_dir)

            # Создаем HTML с несколькими объявлениями
            ads_html = self._create_multi_ad_html_content(ads_data)

            # Создаем полный HTML
            html_content = f"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Multi Ad Screenshot</title>
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
            padding: 20px 20px 0 20px;
            background-color: white;
            color: #534fd8;
            font-size: 18px;
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
            padding: 0 20px 0 20px;
            background-color: white;
            color: #6d6493;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            padding: 0 20px 0 20px;
            background-color: white;
            color: #4b8e4b;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            padding: 0 20px 0 20px;
            background-color: white;
            color: #000000;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            padding: 0 20px 20px 20px;
            background-color: white;
            color: #000000;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            width: 1000px;
            overflow-x: hidden;
        }}
        
        .ad-container {{
            display: flex;
            width: 1000px;
            background-color: white;
            margin-bottom: 20px;
            border-bottom: 1px solid #e0e0e0;
            padding-bottom: 20px;
        }}
        
        .ad-container:last-child {{
            border-bottom: none;
            margin-bottom: 0;
            padding-bottom: 0;
        }}
        
        .ad-image {{
            width: 300px;
            height: var(--image-height, 300px);
            flex-shrink: 0;
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: white;
            padding: 20px;
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
            flex: 1;
            display: flex;
            flex-direction: column;
            justify-content: center;
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
    {ads_html}
</body>
</html>
            """

            # Создаем временный HTML файл
            with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
                f.write(html_content)
                temp_html_path = f.name

            # Сохраняем HTML для отладки
            cur_dir_path = os.path.dirname(__file__)
            debug_html_path = os.path.join(cur_dir_path, f"debug_multi_{screenshot_index}.html")
            with open(debug_html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"      🔍 HTML сохранен для отладки: {debug_html_path}")

            try:
                # Загружаем HTML в браузер
                self.driver.get(f"file://{temp_html_path}")

                # Ждем загрузки контента
                self.driver.implicitly_wait(2)

                # Устанавливаем базовый размер для измерения
                self.driver.set_window_size(1000, 300)

                # Ждем загрузки всех изображений
                print(f"      🖼️ Ждем загрузки изображений...")
                self.driver.implicitly_wait(3)

                # Получаем все контейнеры объявлений
                ad_containers = self.driver.find_elements("class name", "ad-container")
                print(f"      📊 Найдено контейнеров объявлений: {len(ad_containers)}")

                # Рассчитываем общую высоту всех объявлений
                total_height = 0
                for i, container in enumerate(ad_containers):
                    container_size = container.size
                    total_height += container_size['height']
                    print(f"        Объявление {i + 1}: {container_size['height']}px")

                # Увеличиваем высоту в 1.6 раза для надежности
                safe_height = int(total_height * 1.6)
                print(f"      📏 Общая высота всех объявлений: {total_height}px")
                print(f"      📏 Безопасная высота (x1.6): {safe_height}px")

                # Устанавливаем увеличенную высоту окна
                self.driver.set_window_size(1000, safe_height)

                # Принудительно устанавливаем размер viewport
                self.driver.execute_script(
                    "document.body.style.width='1000px'; document.body.style.margin='0'; document.body.style.padding='0';")

                # Ждем финальной загрузки
                self.driver.implicitly_wait(1)

                # Получаем финальные размеры body
                body_element = self.driver.find_element("tag name", "body")
                final_body_size = body_element.size

                print(f"      📏 Финальные размеры body: {final_body_size}")

                # Делаем скриншот всего body
                screenshot = body_element.screenshot_as_png

                # Сохраняем файл
                filename = f"{screenshot_index}.png"
                filepath = os.path.join(output_dir, filename)

                with open(filepath, 'wb') as f:
                    f.write(screenshot)

                print(f"✓ Создан скриншот с {len(ads_data)} объявлениями: {filename} (размер: {safe_height}px)")
                return filepath

            finally:
                # Удаляем временный файл
                try:
                    os.unlink(temp_html_path)
                except:
                    pass

        except Exception as e:
            print(f"✗ Ошибка при создании скриншота с несколькими объявлениями: {e}")
            return None

    def generate_ad_screenshot(self, ad_data: Dict, ad_index: int, output_dir: str = "screenshots",
                               sitelinks_data: Dict = None, extensions_data: Dict = None,
                               image_data: Dict = None) -> str:
        """Генерировать скриншот объявления с использованием HTML"""
        try:
            # Создаем папку для скриншотов, если её нет
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            # Получаем данные объявления
            ad_id = ad_data.get('Id', f'unknown_{ad_index}')

            # Пытаемся получить данные из TextAd, если их нет - используем заглушки
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
                return self._generate_fallback_screenshot(ad_data, ad_index, output_dir)

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

            # Сохраняем HTML для отладки
            cur_dir_path = os.path.dirname(__file__)
            debug_html_path = os.path.join(cur_dir_path, f"debug_{ad_index + 1}_{ad_id}.html")
            with open(debug_html_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"      🔍 HTML сохранен для отладки: {debug_html_path}")

            try:
                # Загружаем HTML в браузер
                self.driver.get(f"file://{temp_html_path}")

                # Ждем загрузки контента
                self.driver.implicitly_wait(2)

                # Сначала устанавливаем базовый размер для измерения
                self.driver.set_window_size(1000, 300)

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
                                  extensions_size['height'] + 40)  # Отступы

                # Высота изображения = высота текстового контейнера (но не больше 300px)
                image_height = min(content_height, 300)

                # Общая высота = высота изображения + высота контента
                total_height = image_height + content_height

                print(f"      📏 Размеры элементов:")
                print(f"        - ad-content: {ad_size['height']}px")
                print(f"        - sitelinks: {sitelinks_size['height']}px")
                print(f"        - url: {url_size['height']}px")
                print(f"        - ad-text: {text_size['height']}px")
                print(f"        - extensions: {extensions_size['height']}px")
                print(f"        - content total: {content_height}px")
                print(f"        - image height (адаптивная): {image_height}px (макс. 300px)")
                print(f"        - total: {total_height}px")

                # Устанавливаем точную высоту окна
                self.driver.set_window_size(1000, total_height)

                # Принудительно устанавливаем размер viewport и высоту изображения
                self.driver.execute_script(f"""
                    document.body.style.width='1000px'; 
                    document.body.style.margin='0'; 
                    document.body.style.padding='0';
                    document.documentElement.style.setProperty('--image-height', '{image_height}px');
                """)

                # Ждем финальной загрузки
                self.driver.implicitly_wait(1)

                # Получаем финальные размеры body
                body_element = self.driver.find_element("tag name", "body")
                final_body_size = body_element.size

                print(f"      📏 Финальные размеры body: {final_body_size}")

                # Делаем скриншот всего body
                screenshot = body_element.screenshot_as_png

                # Сохраняем файл
                filename = f"{ad_index + 1}_{ad_id}.png"
                filepath = os.path.join(output_dir, filename)

                with open(filepath, 'wb') as f:
                    f.write(screenshot)

                print(f"✓ Создан HTML скриншот: {filename} (размер: {total_height}px)")
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
            return self._generate_fallback_screenshot(ad_data, ad_index, output_dir)

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

    def _create_multi_ad_html_content(self, ads_data: List[Dict]) -> str:
        """Создание HTML контента для нескольких объявлений на одном скриншоте"""
        ads_html = ""

        for i, ad_info in enumerate(ads_data):
            ad_data = ad_info['ad_data']
            sitelinks = ad_info.get('sitelinks', [])
            extensions_data = ad_info.get('extensions_data')
            image_url = ad_info.get('image_url')

            # Получаем данные объявления
            text_ad = ad_data.get('TextAd', {})

            if text_ad:
                title = text_ad.get('Title', 'Заголовок не найден')
                title2 = text_ad.get('Title2', 'Подзаголовок не найден')
                title = str(title).encode('utf-8').decode('utf-8')
                title2 = str(title2).encode('utf-8').decode('utf-8')
                display_text = f"{title} - {title2}"
            else:
                ad_id = ad_data.get('Id', f'unknown_{i}')
                ad_type = ad_data.get('Type', 'Неизвестный тип')
                display_text = f"Объявление ID: {ad_id} - Тип: {ad_type}"

            # Экранируем HTML символы
            safe_text = display_text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"',
                                                                                                             '&quot;')

            # Формируем sitelinks HTML
            sitelinks_html = ""
            if sitelinks:
                sitelinks_titles = []
                for sitelink in sitelinks:
                    title = sitelink.get('Title', '')
                    if title:
                        safe_title = title.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"',
                                                                                                                   '&quot;')
                        sitelinks_titles.append(safe_title)

                if sitelinks_titles:
                    sitelinks_text = "     ".join(sitelinks_titles)
                    sitelinks_html = f'<div id="sitelinks">{sitelinks_text}</div>'

            # Формируем URL строку
            url_html = ""
            if text_ad:
                href = text_ad.get('Href', '')
                display_url_path = text_ad.get('DisplayUrlPath', '')

                if href and display_url_path:
                    safe_href = href.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"',
                                                                                                             '&quot;')
                    safe_display_url = display_url_path.replace('&', '&amp;').replace('<', '&lt;').replace('>',
                                                                                                           '&gt;').replace(
                        '"', '&quot;')
                    url_text = f"{safe_href} > {safe_display_url}"
                    url_html = f'<div id="url">{url_text}</div>'

            # Формируем текст объявления
            text_html = ""
            if text_ad:
                text_content = text_ad.get('Text', '')
                if text_content:
                    safe_text_content = text_content.replace('&', '&amp;').replace('<', '&lt;').replace('>',
                                                                                                        '&gt;').replace(
                        '"', '&quot;')
                    text_html = f'<div id="ad-text">{safe_text_content}</div>'

            # Формируем HTML для изображения
            image_html = ""
            if image_url:
                image_html = f'<div class="ad-image"><img src="{image_url}" alt="Ad Image"></div>'

            # Формируем расширения (extensions)
            extensions_html = ""
            if extensions_data:
                ad_extensions = text_ad.get('AdExtensions', [])
                if ad_extensions:
                    extension_ids = [ext.get('AdExtensionId') for ext in ad_extensions if ext.get('AdExtensionId')]
                    all_extensions = extensions_data.get('batch_1', {}).get('result', {}).get('AdExtensions', [])

                    matching_extensions = []
                    for ext in all_extensions:
                        if ext.get('Id') in extension_ids:
                            callout = ext.get('Callout', {})
                            callout_text = callout.get('CalloutText', '')
                            if callout_text:
                                matching_extensions.append(callout_text)

                    if matching_extensions:
                        extensions_text = " · ".join(matching_extensions)
                        extensions_html = f'<div id="extensions">{extensions_text}</div>'

            # Добавляем HTML для одного объявления
            ads_html += f"""
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
            """

        return ads_html

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
            padding: 20px 20px 0 20px;
            background-color: white;
            color: #534fd8;
            font-size: 18px;
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
            padding: 0 20px 0 20px;
            background-color: white;
            color: #6d6493;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            padding: 0 20px 0 20px;
            background-color: white;
            color: #4b8e4b;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            padding: 0 20px 0 20px;
            background-color: white;
            color: #000000;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            padding: 0 20px 20px 20px;
            background-color: white;
            color: #000000;
            font-size: 14px;
            font-weight: normal;
            line-height: 1.5;
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
            width: 1000px;
            overflow-x: hidden;
        }}
        
        .ad-container {{
            display: flex;
            width: 1000px;
            background-color: white;
            margin-bottom: 20px;
            border-bottom: 1px solid #e0e0e0;
            padding-bottom: 20px;
        }}
        
        .ad-container:last-child {{
            border-bottom: none;
            margin-bottom: 0;
            padding-bottom: 0;
        }}
        
        .ad-image {{
            width: 300px;
            height: var(--image-height, 300px);
            flex-shrink: 0;
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: white;
            padding: 20px;
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
            flex: 1;
            display: flex;
            flex-direction: column;
            justify-content: center;
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

    def _generate_fallback_multi_screenshot(self, ads_data: List[Dict], screenshot_index: int, output_dir: str) -> str:
        """Fallback метод генерации скриншота с несколькими объявлениями с использованием PIL"""
        try:
            # Размеры изображения (fallback метод)
            width = 1000
            height = 200 * len(ads_data)  # 200px на объявление

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
                            font = ImageFont.truetype(font_path, 20)
                            break
                        except:
                            continue

                if font is None:
                    font = ImageFont.load_default()

            except Exception as e:
                font = ImageFont.load_default()

            # Цвет текста #000080 (темно-синий)
            text_color = (0, 0, 128)

            # Рисуем текст для каждого объявления
            y_offset = 20
            for i, ad_info in enumerate(ads_data):
                ad_data = ad_info['ad_data']

                # Получаем данные объявления
                text_ad = ad_data.get('TextAd', {})

                if text_ad:
                    title = text_ad.get('Title', 'Заголовок не найден')
                    title2 = text_ad.get('Title2', 'Подзаголовок не найден')
                    title = str(title).encode('utf-8').decode('utf-8')
                    title2 = str(title2).encode('utf-8').decode('utf-8')
                    display_text = f"{title} - {title2}"
                else:
                    ad_id = ad_data.get('Id', f'unknown_{i}')
                    ad_type = ad_data.get('Type', 'Неизвестный тип')
                    display_text = f"Объявление ID: {ad_id} - Тип: {ad_type}"

                # Рисуем текст
                draw.text((20, y_offset), f"Объявление {i + 1}: {display_text}", fill=text_color, font=font)
                y_offset += 180  # Отступ между объявлениями

            # Сохраняем файл
            filename = f"{screenshot_index}.png"
            filepath = os.path.join(output_dir, filename)
            img.save(filepath)

            print(f"✓ Создан fallback скриншот с {len(ads_data)} объявлениями: {filename}")
            return filepath

        except Exception as e:
            print(f"✗ Ошибка при создании fallback скриншота: {e}")
            return None

    def _generate_fallback_screenshot(self, ad_data: Dict, ad_index: int, output_dir: str) -> str:
        """Fallback метод генерации скриншота с использованием PIL"""
        try:
            # Получаем данные объявления
            ad_id = ad_data.get('Id', f'unknown_{ad_index}')

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

            # Размеры изображения (fallback метод)
            width = 900
            height = 200

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
                            font = ImageFont.truetype(font_path, 20)
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

            # Сохраняем файл
            filename = f"{ad_index + 1}_{ad_id}.png"
            filepath = os.path.join(output_dir, filename)
            img.save(filepath)

            print(f"✓ Создан fallback скриншот: {filename}")
            return filepath

        except Exception as e:
            print(f"✗ Ошибка при создании fallback скриншота: {e}")
            return None

    def process_report(self, report: Dict) -> (io.BytesIO, str):
        """Обработать один отчет"""
        print(f"\n{'=' * 60}")
        print(f"ОБРАБОТКА ОТЧЕТА #{report['id']}")
        print(f"{'=' * 60}")
        print(f"ID отчета: {report['id']}")
        print(f"ID договора: {report['id_contracts']}")
        print(f"Номер договора: {report['number_contract']}")
        print(f"Предмет договора: {report['subject_contract']}")

        # Загружаем данные из MinIO
        print(f"\nЗагрузка данных из MinIO...")
        data = self.load_data_from_minio(report['id'])

        # Обрабатываем данные объявлений
        ads_report_key = f'ads_report_{report["id"]}.json'
        if data.get(ads_report_key):
            ads_data = data[ads_report_key]
            print(f"\n📊 ДАННЫЕ ОБЪЯВЛЕНИЙ:")
            print(f"Количество объявлений: {len(ads_data.get('result', {}).get('Ads', []))}")

            # Получаем объявления
            ads = ads_data.get('result', {}).get('Ads', [])

            # ВРЕМЕННЫЙ ХАРДКОД: обрабатываем только первые 3 объявления для тестирования
            # TODO: Убрать хардкод и обрабатывать все объявления
            # ads_to_process = ads[:3]
            # ads_to_process = ads[53:56]
            ads_to_process = ads
            print(f"🔧 ХАРДКОД: Обрабатываем только первые {len(ads_to_process)} объявлений из {len(ads)}")

            print(f"\n🖼️ ГЕНЕРАЦИЯ СКРИНШОТОВ:")
            print(f"📊 Обрабатываем {len(ads_to_process)} объявлений по {self.ads_per_screenshot} на скриншот")

            # Группируем объявления по ads_per_screenshot
            screenshot_index = 1
            for i in range(0, len(ads_to_process), self.ads_per_screenshot):
                # Получаем группу объявлений для одного скриншота
                ads_group = ads_to_process[i:i + self.ads_per_screenshot]
                print(f"\n📸 Создание скриншота #{screenshot_index} с {len(ads_group)} объявлениями:")

                # Подготавливаем данные для каждого объявления в группе
                ads_data = []
                for j, ad in enumerate(ads_group):
                    print(f"  Объявление {j + 1}: ID {ad.get('Id')} - {ad.get('Type')}")

                    # Получаем данные sitelinks, extensions и изображений
                    sitelinks_data = data.get(f'sitelinks_{report["id"]}.json')
                    extensions_data = data.get(f'extensions_{report["id"]}.json')
                    image_data = data.get(f'image_hashes_report_{report["id"]}.json')

                    # Получаем sitelinks для объявления
                    sitelinks = self._get_sitelinks_for_ad(ad, sitelinks_data)

                    # Получаем изображение для объявления
                    image_url = self._get_image_for_ad(ad, image_data)

                    # Добавляем данные объявления в группу
                    ads_data.append({
                        'ad_data': ad,
                        'sitelinks': sitelinks,
                        'extensions_data': extensions_data,
                        'image_url': image_url
                    })

                # Генерируем скриншот с группой объявлений
                screenshot_path = self.generate_multi_ad_screenshot(ads_data, screenshot_index, "screenshots")
                if screenshot_path:
                    print(f"  ✅ Скриншот #{screenshot_index} сохранен: {screenshot_path}")
                else:
                    print(f"  ❌ Ошибка создания скриншота #{screenshot_index}")

                screenshot_index += 1

        # Обрабатываем данные расширений
        extensions_key = f'extensions_{report["id"]}.json'
        if data.get(extensions_key):
            extensions_data = data[extensions_key]
            print(f"\n🔗 РАСШИРЕНИЯ:")
            extensions = extensions_data.get('batch_1', {}).get('result', {}).get('AdExtensions', [])
            print(f"Количество расширений: {len(extensions)}")

            for i, ext in enumerate(extensions[:2]):  # Показываем первые 2 расширения
                print(f"\nРасширение #{i + 1}:")
                print(f"  ID: {ext.get('Id')}")
                print(f"  Тип: {ext.get('Type')}")
                if ext.get('Callout'):
                    print(f"  Текст: {ext.get('Callout', {}).get('CalloutText')}")

        # Обрабатываем данные изображений
        images_key = f'image_hashes_report_{report["id"]}.json'
        if data.get(images_key):
            images_data = data[images_key]
            print(f"\n🖼️ ИЗОБРАЖЕНИЯ:")
            images = images_data.get('result', {}).get('AdImages', [])
            print(f"Количество изображений: {len(images)}")

            for i, img in enumerate(images[:2]):  # Показываем первые 2 изображения
                print(f"\nИзображение #{i + 1}:")
                print(f"  Название: {img.get('Name')}")
                print(f"  Тип: {img.get('Type')}")
                print(f"  Хеш: {img.get('AdImageHash')}")
                print(f"  Связано: {img.get('Associated')}")

        # Обрабатываем данные ключевых слов
        keywords_key = f'keywords_traffic_forecast_{report["id"]}.json'
        if data.get(keywords_key):
            keywords_data = data[keywords_key]
            print(f"\n🔑 КЛЮЧЕВЫЕ СЛОВА:")
            keywords = keywords_data.get('result', {}).get('Keywords', [])
            print(f"Количество ключевых слов: {len(keywords)}")

            # Показываем первые несколько ключевых слов
            for i, keyword in enumerate(keywords[:5]):  # Показываем первые 5 ключевых слов
                print(f"  {i + 1}. {keyword.get('Keyword')} (ID: {keyword.get('Id')})")

        # Обрабатываем данные быстрых ссылок
        sitelinks_key = f'sitelinks_{report["id"]}.json'
        if data.get(sitelinks_key):
            sitelinks_data = data[sitelinks_key]
            print(f"\n🔗 БЫСТРЫЕ ССЫЛКИ:")
            sitelinks_sets = sitelinks_data.get('result', {}).get('SitelinksSets', [])
            print(f"Количество наборов быстрых ссылок: {len(sitelinks_sets)}")

            for i, sitelinks_set in enumerate(sitelinks_sets[:1]):  # Показываем первый набор
                print(f"\nНабор быстрых ссылок #{i + 1}:")
                print(f"  ID набора: {sitelinks_set.get('Id')}")
                sitelinks = sitelinks_set.get('Sitelinks', [])
                print(f"  Количество ссылок: {len(sitelinks)}")

                for j, link in enumerate(sitelinks[:3]):  # Показываем первые 3 ссылки
                    print(f"    {j + 1}. {link.get('Title')} - {link.get('Description')}")
                    print(f"       URL: {link.get('Href')}")

        # упаковка скриншотов в архив
        report_id = report.get('id')
        screens_zip_file = create_and_packaging_zip(report_id)

        return screens_zip_file, screens_zip_file.name

    def run(self):
        """Основной метод запуска обработки"""
        print("🚀 Запуск генератора скриншотов объявлений...")

        # Получаем отчеты для обработки
        reports = self.get_pending_reports()

        if not reports:
            print("📭 Нет отчетов со статусом 1 для обработки")
            return

        print(f"📋 Найдено отчетов для обработки: {len(reports)}")

        # Обрабатываем каждый отчет
        for report in reports:
            try:
                self.process_report(report)


            except Exception as e:
                print(f"❌ Ошибка при обработке отчета {report['id']}: {e}")
                continue

        print(f"\n✅ Обработка завершена. Обработано отчетов: {len(reports)}")


def generate_screens_ads(report_id):
    """Главная функция"""
    try:
        generator = AdScreenshotsGenerator()

        from utils.postprocessing_report_file import get_report_by_id
        report = get_report_by_id(report_id)

        screens_file, filename = generator.process_report(report)
        html_remove()
        return screens_file, filename
        # generator.run()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        raise e


if __name__ == "__main__":
    generate_screens_ads(16)
