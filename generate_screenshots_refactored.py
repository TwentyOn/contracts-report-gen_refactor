#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для создания скриншотов отчетов Яндекс.Директ
Использует общие модули database_manager и minio_client
"""

import os
import json
import random
import time
import zipfile
import shutil
import subprocess
import psutil
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from PIL import Image, ImageDraw, ImageFont
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from database_manager import DatabaseManager
from minio_client import MinIOClient

# Загружаем переменные окружения
load_dotenv('.env')

IS_WINDOWS = os.getenv("IS_WINDOWS", "False").lower() in ("1", "true", "yes")
platform_suffix = "windows" if IS_WINDOWS else "linux"


class ScreenshotGenerator:
    """Генератор скриншотов отчетов Яндекс.Директ"""

    def __init__(self):
        self.db = DatabaseManager()
        self.minio_client = MinIOClient()
        self.current_report_id = None

        # Пути к медиа файлам (копируем из оригинального скрипта)
        self.media_dir = os.path.join(os.path.dirname(__file__), 'media')
        self.panel_path = os.path.join(self.media_dir, 'panel.png')
        self.up_panel_path = os.path.join(self.media_dir, 'up_panel.png')
        self.font_path = os.path.join(self.media_dir, 'segoeui.ttf')

        # Создаем медиа папку если её нет
        os.makedirs(self.media_dir, exist_ok=True)

    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск генерации скриншотов")
        print("=" * 60)

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
                self.process_single_report(report)

            return True

        finally:
            self.db.disconnect()

    def process_single_report(self, report: Dict):
        """Обрабатывает один отчет"""
        try:
            self.current_report_id = report['id']

            # Загружаем URL из MinIO
            urls_data = self.load_urls_from_minio(report['id'])
            if not urls_data:
                print(f"❌ Не удалось загрузить URL для отчета {report['id']}")
                return

            urls = [url_info['url'] for url_info in urls_data.get('urls', [])]
            if not urls:
                print(f"❌ Нет URL для обработки в отчете {report['id']}")
                return

            print(f"📊 Найдено URL для скриншотов: {len(urls)}")

            # ID пользователя (можно сделать динамическим из БД)
            user_id = 1

            print(f"🔑 Используем профиль пользователя: {user_id}")

            # Генерируем скриншоты
            result = self.generate_screenshots(user_id, urls, report['id'])

            print(f"📸 Результат генерации скриншотов: {result}")

            if result == "OK":
                print("✅ Все скриншоты успешно созданы!")
            elif result == "PARTIAL_SUCCESS":
                print("⚠️ Частичный успех - некоторые скриншоты созданы")
            elif result == "ALL_FAILED":
                print("❌ Все URL не удалось обработать")
            elif result == "OLD_COOKIES":
                print("🔄 Требуется обновление куки - авторизация истекла")
            elif result == "PROFILE_ERROR":
                print("❌ Ошибка при загрузке профиля пользователя")
            else:
                print(f"❓ Неизвестный результат: {result}")

        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")

    def load_urls_from_minio(self, report_id: int) -> Optional[Dict]:
        """Загружает URL из файла в MinIO"""
        try:
            # Ищем файл report_urls_{report_id}.json в MinIO
            prefix = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/"
            objects = self.minio_client.list_objects(prefix)

            # Фильтруем только файлы report_urls_
            urls_files = [obj for obj in objects if f"report_urls_{report_id}.json" in obj]

            if not urls_files:
                print(f"❌ Файл report_urls_{report_id}.json не найден в MinIO")
                return None

            # Берем первый найденный файл
            latest_file = urls_files[0]
            print(f"📁 Загружаем URL из файла в MinIO: {latest_file}")

            # Загружаем данные из MinIO
            response = self.minio_client.client.get_object(
                self.minio_client.bucket_name,
                latest_file
            )
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()

            print(f"✅ Загружено URL из MinIO: {len(data.get('urls', []))}")
            return data

        except Exception as e:
            print(f"❌ Ошибка загрузки URL из MinIO: {e}")
            return None

    def kill_chrome_processes(self):
        """Принудительно завершает все процессы Chrome"""
        try:
            for proc in psutil.process_iter(['pid', 'name']):
                if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                    try:
                        proc.terminate()
                        proc.wait(timeout=3)
                    except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                        try:
                            proc.kill()
                        except psutil.NoSuchProcess:
                            pass
            time.sleep(2)  # Даем время процессам завершиться
        except Exception as e:
            print(f"⚠️ Ошибка при завершении процессов Chrome: {e}")

    def create_driver(self, user_id: int):
        """Создает Chrome драйвер с настройками"""
        options = Options()
        options.add_argument("start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-blink-features=AutomationControlled")

        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_directory = os.path.join(script_dir, 'users')
        user_directory = os.path.join(base_directory, f'user_{user_id}')

        options.add_argument(f'user-data-dir={user_directory}')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        options.add_argument('--no-sandbox')
        options.add_argument('--headless=new')

        # service = Service(ChromeDriverManager().install())

        driver = webdriver.Chrome(options=options)
        stealth(driver,
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'",
                languages=["ru-RU", "ru"],
                vendor="Google Inc.",
                platform="Linux x86_64",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                run_on_insecure_origins=True
                )
        return driver

    def download_profile_from_minio(self, user_id: int):
        """Загружает профиль пользователя из MinIO с повторными попытками"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        user_dir = os.path.join(script_dir, "users")
        archive_name = f"user_{user_id}_{platform_suffix}.zip"
        # archive_path = script_dir / archive_name
        os.makedirs(user_dir, exist_ok=True)

        # archive_name = f"user_{user_id}.zip"
        archive_path = os.path.join(script_dir, archive_name)
        object_name = f"users_for_screenshots/{archive_name}"

        # Настройки для повторных попыток
        max_retries = 3
        retry_delay = 5  # секунд между попытками

        for attempt in range(max_retries):
            try:
                print(f"🔄 Попытка загрузки профиля {attempt + 1}/{max_retries}")

                # Удаляем старый файл если он есть
                if os.path.exists(archive_path):
                    os.remove(archive_path)

                # Загружаем файл с увеличенным таймаутом
                self.minio_client.client.fget_object(
                    self.minio_client.bucket_name,
                    object_name,
                    archive_path
                )

                # Проверяем, что файл загрузился полностью
                if os.path.exists(archive_path) and os.path.getsize(archive_path) > 0:
                    print(f"✅ Профиль загружен успешно (размер: {os.path.getsize(archive_path)} байт)")
                    break
                else:
                    raise Exception("Файл не загружен или пустой")

            except Exception as e:
                print(f"❌ Ошибка загрузки профиля (попытка {attempt + 1}): {e}")

                if attempt < max_retries - 1:
                    print(f"⏳ Ожидание {retry_delay} секунд перед повторной попыткой...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Увеличиваем задержку с каждой попыткой
                else:
                    print("❌ Все попытки загрузки профиля исчерпаны")
                    raise Exception(f"Не удалось загрузить профиль после {max_retries} попыток: {e}")

        # Распаковываем архив
        try:
            with zipfile.ZipFile(archive_path, "r") as zipf:
                zipf.extractall(user_dir)
            print("✅ Профиль распакован успешно")
        except Exception as e:
            print(f"❌ Ошибка распаковки профиля: {e}")
            raise

        # Удаляем архив
        try:
            os.remove(archive_path)
            print("🗑️ Временный архив удален")
        except Exception as e:
            print(f"⚠️ Не удалось удалить временный архив: {e}")

    def add_panel_with_time(self, img: Image.Image) -> Image.Image:
        """Добавляет панель с временем и датой к изображению"""
        try:
            # Проверяем существование медиа файлов
            if not os.path.exists(self.panel_path) or not os.path.exists(self.up_panel_path):
                print("⚠️ Медиа файлы не найдены, возвращаем изображение без панели")
                return img

            panel = Image.open(self.panel_path).convert("RGBA")
            up_panel = Image.open(self.up_panel_path).convert("RGBA")

            panel = panel.resize((img.width, panel.height))
            up_panel = up_panel.resize((img.width, up_panel.height))

            new_height = img.height + panel.height + up_panel.height
            new_img = Image.new("RGB", (img.width, new_height), (255, 255, 255))

            new_img.paste(up_panel, (0, 0), up_panel)
            new_img.paste(img, (0, up_panel.height))
            panel_y = up_panel.height + img.height
            new_img.paste(panel, (0, panel_y), panel)

            current_time = time.strftime("%H:%M")
            current_date = time.strftime("%d.%m.%Y")
            draw = ImageDraw.Draw(new_img)
            panel_color = (223, 231, 243)

            # Проверяем существование шрифта
            if os.path.exists(self.font_path):
                font_size = 15
                font = ImageFont.truetype(self.font_path, font_size)
            else:
                print("⚠️ Шрифт не найден, используем стандартный")
                font = ImageFont.load_default()

            bbox_t = draw.textbbox((0, 0), current_time, font=font)
            tw, th = bbox_t[2] - bbox_t[0], bbox_t[3] - bbox_t[1]
            bbox_d = draw.textbbox((0, 0), current_date, font=font)
            dw, dh = bbox_d[2] - bbox_d[0], bbox_d[3] - bbox_d[1]

            bbox_digits = draw.textbbox((0, 0), "00", font=font)
            two_digit_w = bbox_digits[2] - bbox_digits[0]
            right_padding = two_digit_w + 6

            rect_w = max(tw, dw) + right_padding + 6
            rect_x = new_img.width - rect_w
            rect_y = new_img.height - panel.height + 2
            rect_h = panel.height - 4
            draw.rectangle((rect_x, rect_y, new_img.width, rect_y + rect_h), fill=panel_color)

            text_x_time = new_img.width - tw - (right_padding - 4)
            text_y_time = rect_y + 6
            gap = 8
            text_x_date = new_img.width - dw - (right_padding - 4)
            text_y_date = text_y_time + th + gap

            draw.text((text_x_time, text_y_time), current_time, font=font, fill="black")
            draw.text((text_x_date, text_y_date), current_date, font=font, fill="black")
            return new_img

        except Exception as e:
            print(f"⚠️ Ошибка добавления панели: {e}, возвращаем исходное изображение")
            return img

    def scroll_and_screenshot(self, driver, output_dir: str, url_index: int):
        """Выполняет скроллинг и создает скриншоты"""
        print(f"📁 Создаем папку: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)
        driver.set_window_size(1920, 1080)

        screenshot_index = 1
        overlap = 200
        left_margin = 200
        window_height = driver.get_window_size()["height"]

        while True:
            action = ActionChains(driver)
            action.move_by_offset(random.randint(1, 10), random.randint(1, 10)).perform()
            block_selector = (
                "body > div.b-page__content-wrapper-with-sidebar > "
                "div.b-page__wrapper > div.b-page__content > div > "
                "table > tbody > tr:nth-child(5) > td.l-page__center"
            )
            print(f"🔍 Ищем элемент: {block_selector}")

            try:
                block = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, block_selector))
                )
                print("✅ Элемент найден, начинаем скриншоты")
            except TimeoutException:
                print("⏳ Не удалось дождаться появления элемента")
                return False
            except Exception as e:
                print(f"❌ Ошибка при поиске элемента: {e}")
                return False

            block_y = block.location["y"]
            block_height = block.size["height"]
            step = window_height - overlap
            current_pos = 0

            while current_pos < block_height:
                if current_pos + step >= block_height:
                    scroll_pos = block_y + max(current_pos, block_height - window_height)
                    driver.execute_script(f"window.scrollTo(0, {scroll_pos});")
                    path = os.path.join(output_dir, f"screenshot_{screenshot_index:03}.png")
                    driver.save_screenshot(path)

                    img = Image.open(path)
                    width, height = img.size
                    img_cropped = img.crop((left_margin, 0, width, height))
                    final_img = self.add_panel_with_time(img_cropped)
                    final_img.save(path)

                    screenshot_index += 1
                    break

                driver.execute_script(f"window.scrollTo(0, {block_y + current_pos});")
                path = os.path.join(output_dir, f"screenshot_{screenshot_index:03}.png")
                driver.save_screenshot(path)
                img = Image.open(path)
                width, height = img.size
                img_cropped = img.crop((left_margin, 0, width, height))
                final_img = self.add_panel_with_time(img_cropped)
                final_img.save(path)
                current_pos += step
                screenshot_index += 1

            try:
                next_button = driver.find_element(By.CSS_SELECTOR, "a.b-pager__next")
                if "disabled" in next_button.get_attribute("class"):
                    break
                else:
                    next_button.click()
                    time.sleep(1)
            except NoSuchElementException:
                break

        driver.execute_script("window.scrollTo(0, 0);")
        return True

    def upload_screenshots_to_minio(self, screenshots_dir: str, report_id: int, url_index: int):
        """Загружает скриншоты в MinIO"""
        try:
            if not os.path.exists(screenshots_dir):
                print(f"❌ Папка скриншотов не найдена: {screenshots_dir}")
                return False

            # Получаем список файлов скриншотов
            screenshot_files = [f for f in os.listdir(screenshots_dir) if f.endswith('.png')]

            if not screenshot_files:
                print(f"❌ Нет скриншотов для загрузки в папке: {screenshots_dir}")
                return False

            print(f"📤 Загружаем {len(screenshot_files)} скриншотов в MinIO для URL {url_index}...")

            # Загружаем каждый скриншот в подпапку url_{url_index}
            for screenshot_file in screenshot_files:
                local_path = os.path.join(screenshots_dir, screenshot_file)
                minio_path = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/screenshots/url_{url_index}/{screenshot_file}"

                # Загружаем файл в MinIO
                with open(local_path, 'rb') as file_data:
                    self.minio_client.client.put_object(
                        self.minio_client.bucket_name,
                        minio_path,
                        file_data,
                        length=os.path.getsize(local_path),
                        content_type='image/png'
                    )

                print(f"✅ Загружен: url_{url_index}/{screenshot_file}")

            # Удаляем локальную папку после загрузки
            shutil.rmtree(screenshots_dir)
            print(f"🗑️ Локальная папка удалена: {screenshots_dir}")

            return True

        except Exception as e:
            print(f"❌ Ошибка загрузки скриншотов в MinIO: {e}")
            return False

    def generate_screenshots(self, user_id: int, urls: List[str], report_id: int) -> str:
        """Генерирует скриншоты для списка URL"""
        print(f"🔑 Начинаем загрузку профиля для пользователя {user_id}")
        try:
            self.download_profile_from_minio(user_id)
            print("✅ Профиль успешно загружен")
        except Exception as e:
            print(f"❌ Ошибка при загрузке профиля: {e}")
            return "PROFILE_ERROR"

        successful_urls = 0
        failed_urls = 0

        for i, url in enumerate(urls, start=1):
            print(f"🌐 Обрабатываем URL {i}: {url[:50]}...")
            driver = None

            try:
                # Очищаем процессы Chrome перед каждым запуском
                if i > 1:
                    print("🧹 Очищаем процессы Chrome...")
                    self.kill_chrome_processes()

                driver = self.create_driver(user_id)
                driver.get(url)
                time.sleep(2)  # Увеличиваем время ожидания

                try:
                    driver.find_element(By.NAME, "login")
                    print("🔐 Найдено поле логина - требуются новые куки")
                    if driver:
                        driver.quit()
                    self.cleanup_user_profile(user_id)
                    return "OLD_COOKIES"
                except NoSuchElementException:
                    print("✅ Поле логина не найдено - продолжаем")

                # Создаем папку для скриншотов
                screenshots_dir = os.path.join(os.getcwd(), "temp_screenshots", f"site_{i}")
                print(f"📁 Создаем папку для скриншотов: {screenshots_dir}")
                success = self.scroll_and_screenshot(driver, screenshots_dir, i)

                if driver:
                    driver.quit()

                if success:
                    # Загружаем скриншоты в MinIO
                    upload_success = self.upload_screenshots_to_minio(screenshots_dir, report_id, i)
                    if upload_success:
                        successful_urls += 1
                        print(f"✅ URL {i} обработан успешно")
                    else:
                        failed_urls += 1
                        print(f"❌ Ошибка загрузки скриншотов для URL {i}")
                else:
                    failed_urls += 1
                    print(f"❌ Ошибка при создании скриншотов для URL {i}")

            except Exception as e:
                print(f"❌ Ошибка при обработке URL {i}: {e}")
                failed_urls += 1
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass  # Игнорируем ошибки при закрытии

        self.cleanup_user_profile(user_id)
        print(f"📊 Скрипт завершен. Успешно: {successful_urls}, Ошибок: {failed_urls}")

        if failed_urls == 0:
            return "OK"
        elif successful_urls > 0:
            return "PARTIAL_SUCCESS"
        else:
            return "ALL_FAILED"

    def cleanup_user_profile(self, user_id: int):
        """Очищает профиль пользователя"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        user_dir = os.path.join(script_dir, "users")
        if os.path.exists(user_dir):
            shutil.rmtree(user_dir)
            print(f"🗑️ Профиль пользователя {user_id} очищен")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта генерации скриншотов")
    print("=" * 60)

    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY',
                     'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return

    # Создаем и запускаем генератор
    generator = ScreenshotGenerator()

    try:
        success = generator.process_reports()
        if success:
            print("\n✅ Генерация скриншотов завершена успешно")
        else:
            print("\n❌ Генерация скриншотов завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()
