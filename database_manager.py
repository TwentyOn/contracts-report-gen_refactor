#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Общий модуль для работы с базой данных
Содержит классы для подключения к БД и получения данных
"""

import os
import json
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv('.env')

class DatabaseManager:
    """Менеджер для работы с базой данных"""
    
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Подключается к базе данных"""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            self.cursor = self.connection.cursor()
            print("✅ Подключение к БД установлено")
            return True
        except Exception as e:
            print(f"❌ Ошибка подключения к БД: {e}")
            return False
    
    def disconnect(self):
        """Отключается от базы данных"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("🔌 Соединение с БД закрыто")
    
    def get_yandex_accounts(self) -> List[Dict]:
        """Получает аккаунты Яндекс.Директ из БД"""
        try:
            query = """
                SELECT id, direct_api_token, client_id, client_secret, comment
                FROM gen_report_context_contracts.yandexdirectaccounts 
                WHERE is_deleted IS NULL OR is_deleted = false
                ORDER BY id
            """
            self.cursor.execute(query)
            accounts = []
            for row in self.cursor.fetchall():
                accounts.append({
                    'id': row[0],
                    'direct_api_token': row[1],
                    'client_id': row[2],
                    'client_secret': row[3],
                    'comment': row[4]
                })
            print(f"📊 Найдено аккаунтов Яндекс.Директ: {len(accounts)}")
            return accounts
        except Exception as e:
            print(f"❌ Ошибка получения аккаунтов: {e}")
            return []
    
    def get_wordstat_accounts(self) -> List[Dict]:
        """Получает аккаунты Wordstat API из БД"""
        try:
            query = """
                SELECT id, wordstat_login, wordstat_token, client_id, client_secret, comment
                FROM gen_report_context_contracts.wordstatapiaccounts 
                WHERE is_deleted IS NULL OR is_deleted = false
                ORDER BY id
            """
            self.cursor.execute(query)
            accounts = []
            for row in self.cursor.fetchall():
                accounts.append({
                    'id': row[0],
                    'wordstat_login': row[1],
                    'wordstat_token': row[2],
                    'client_id': row[3],
                    'client_secret': row[4],
                    'comment': row[5]
                })
            print(f"📊 Найдено аккаунтов Wordstat API: {len(accounts)}")
            return accounts
        except Exception as e:
            print(f"❌ Ошибка получения аккаунтов Wordstat: {e}")
            return []
    
    def get_reports_to_process(self) -> List[Dict]:
        """Получает отчеты со статусом 1 для обработки"""
        try:
            query = """
                SELECT r.id, r.id_requests, r.id_contracts, r.message
                FROM gen_report_context_contracts.reports r
                WHERE r.id_status = 1 
                AND (r.is_deleted IS NULL OR r.is_deleted = false)
                ORDER BY r.id
            """
            self.cursor.execute(query)
            reports = []
            for row in self.cursor.fetchall():
                reports.append({
                    'id': row[0],
                    'id_requests': row[1],
                    'id_contracts': row[2],
                    'message': row[3]
                })
            print(f"📊 Найдено отчетов для обработки: {len(reports)}")
            return reports
        except Exception as e:
            print(f"❌ Ошибка получения отчетов: {e}")
            return []
    
    def get_request_data(self, request_id: int) -> Optional[Dict]:
        """Получает данные заявки по ID"""
        try:
            query = """
                SELECT id, id_contracts, campany_yandex_direct, deleted_groups
                FROM gen_report_context_contracts.requests 
                WHERE id = %s
            """
            self.cursor.execute(query, (request_id,))
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'id_contracts': row[1],
                    'campany_yandex_direct': row[2],
                    'deleted_groups': row[3]
                }
            return None
        except Exception as e:
            print(f"❌ Ошибка получения данных заявки: {e}")
            return None
    
    def get_contract_data(self, contract_id: int) -> Optional[Dict]:
        """Получает данные договора по ID"""
        try:
            query = """
                SELECT id, login_yandex_direct
                FROM gen_report_context_contracts.contracts 
                WHERE id = %s
            """
            self.cursor.execute(query, (contract_id,))
            row = self.cursor.fetchone()
            if row:
                return {
                    'id': row[0],
                    'login_yandex_direct': row[1]
                }
            return None
        except Exception as e:
            print(f"❌ Ошибка получения данных договора: {e}")
            return None
    
    def extract_campaign_ids(self, campaign_data: Dict) -> List[int]:
        """Извлекает ID кампаний из JSON данных"""
        try:
            if isinstance(campaign_data, str):
                campaign_data = json.loads(campaign_data)
            
            campaigns = campaign_data.get('campaigns', [])
            campaign_ids = []
            
            for campaign in campaigns:
                if 'id' in campaign:
                    campaign_ids.append(campaign['id'])
            
            return campaign_ids
        except Exception as e:
            print(f"❌ Ошибка извлечения ID кампаний: {e}")
            return []
    
    def load_keywords_from_minio(self, minio_client, report_id: int) -> List[str]:
        """Загружает ключевые фразы из файла keywords_traffic_forecast_ в MinIO"""
        try:
            # Получаем список объектов с префиксом для текущего отчета
            prefix = f"gen_report_context_contracts/data_yandex_direct/{report_id}_результаты/"
            objects = minio_client.list_objects(prefix)
            
            # Фильтруем только файлы keywords_traffic_forecast_
            keywords_files = [obj for obj in objects if f"keywords_traffic_forecast_{report_id}.json" in obj]
            
            if not keywords_files:
                print("❌ Файл keywords_traffic_forecast_ не найден в MinIO")
                return []
            
            # Берем первый найденный файл (должен быть только один)
            latest_file = keywords_files[0]
            print(f"📁 Загружаем фразы из файла в MinIO: {latest_file}")
            
            # Загружаем данные из MinIO
            response = minio_client.client.get_object(
                minio_client.bucket_name, 
                latest_file
            )
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            
            keywords = []
            if 'result' in data and 'Keywords' in data['result']:
                for keyword_data in data['result']['Keywords']:
                    if 'Keyword' in keyword_data:
                        keyword = keyword_data['Keyword'].strip()
                        # Исключаем "---autotargeting"
                        if keyword != "---autotargeting":
                            keywords.append(keyword)
            
            # Убираем дубликаты, сохраняя порядок
            unique_keywords = []
            seen = set()
            for keyword in keywords:
                if keyword not in seen:
                    unique_keywords.append(keyword)
                    seen.add(keyword)
            
            print(f"📊 Найдено уникальных ключевых фраз: {len(unique_keywords)}")
            return unique_keywords
            
        except Exception as e:
            print(f"❌ Ошибка загрузки ключевых фраз: {e}")
            return []
    
    def check_phrase_freshness(self, phrase: str) -> bool:
        """Проверяет, есть ли свежая фраза в БД (не старше недели)"""
        try:
            print(f"🔍 Проверяем фразу в БД: '{phrase}'")
            
            query = """
                SELECT create_entry FROM gen_report_context_contracts.wordstatkeyphrases 
                WHERE phrase = %s AND is_deleted = false
                ORDER BY create_entry DESC LIMIT 1
            """
            self.cursor.execute(query, (phrase,))
            result = self.cursor.fetchone()
            
            if not result:
                print(f"❌ Фраза '{phrase}' НЕ НАЙДЕНА в БД - нужен API запрос")
                return False  # Фраза не найдена, нужен API запрос
            
            create_time = result[0]
            week_ago = datetime.now() - timedelta(days=7)
            
            is_fresh = create_time > week_ago
            print(f"🕒 Фраза '{phrase}' найдена в БД:")
            print(f"   📅 Время создания: {create_time}")
            print(f"   📅 Неделю назад: {week_ago}")
            print(f"   ✅ Свежая: {is_fresh}")
            
            if is_fresh:
                print(f"⏭️ Фраза '{phrase}' СВЕЖАЯ - пропускаем API запрос")
            else:
                print(f"🔄 Фраза '{phrase}' УСТАРЕЛА - нужен API запрос")
            
            return is_fresh  # True если свежая, False если устарела
            
        except Exception as e:
            print(f"❌ Ошибка проверки свежести фразы '{phrase}': {e}")
            print(f"🔄 В случае ошибки делаем API запрос")
            return False  # В случае ошибки делаем API запрос
    
    def mark_old_phrases_as_deleted(self, phrases_to_delete: List[str]):
        """Помечает старые фразы как удаленные"""
        try:
            if not phrases_to_delete:
                return
            
            placeholders = ','.join(['%s'] * len(phrases_to_delete))
            query = f"""
                UPDATE gen_report_context_contracts.wordstatkeyphrases 
                SET is_deleted = true
                WHERE phrase IN ({placeholders}) AND is_deleted = false
            """
            
            params = phrases_to_delete
            self.cursor.execute(query, params)
            
            print(f"🗑️ Помечено как удаленные: {len(phrases_to_delete)} фраз")
            
        except Exception as e:
            print(f"❌ Ошибка пометки фраз как удаленных: {e}")
    
    def save_phrases_to_db(self, phrases_data: Dict, original_phrase: str):
        """Сохраняет фразы в таблицу wordstatkeyphrases с умным управлением флагами удаления"""
        try:
            print(f"💾 Начинаем сохранение фраз для исходной фразы: '{original_phrase}'")
            print(f"📊 Данные от API: {list(phrases_data.keys())}")
            
            if 'topRequests' not in phrases_data:
                print("⚠️ Нет данных о фразах для сохранения")
                return
            
            saved_count = 0
            skipped_count = 0
            updated_count = 0
            phrases_to_delete = []
            
            # Сначала сохраняем исходную фразу, если она есть в данных
            if 'requestPhrase' in phrases_data and phrases_data['requestPhrase']:
                original_phrase_from_api = phrases_data['requestPhrase']
                total_count = phrases_data.get('totalCount', 0)
                
                print(f"🔍 Проверяем исходную фразу в БД: '{original_phrase_from_api}' (запросов: {total_count})")
                
                # Проверяем, существует ли уже исходная фраза
                check_original_query = """
                    SELECT id, count, create_entry FROM gen_report_context_contracts.wordstatkeyphrases 
                    WHERE phrase = %s AND is_deleted = false
                """
                self.cursor.execute(check_original_query, (original_phrase_from_api,))
                existing_original = self.cursor.fetchone()
                
                if existing_original:
                    existing_id, existing_count, create_time = existing_original
                    week_ago = datetime.now() - timedelta(days=7)
                    is_fresh = create_time > week_ago
                    
                    if not is_fresh:
                        phrases_to_delete.append(original_phrase_from_api)
                        print(f"🔄 Исходная фраза '{original_phrase_from_api}' устарела, обновляем данные")
                        
                        # Вставляем новую запись для исходной фразы
                        insert_original_query = """
                            INSERT INTO gen_report_context_contracts.wordstatkeyphrases 
                            (phrase, regions, devices, count, is_deleted, create_entry)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        
                        self.cursor.execute(insert_original_query, (
                            original_phrase_from_api,
                            '[213]',  # Москва
                            '["all"]',  # Все устройства
                            total_count,  # Общее количество запросов
                            False,
                            datetime.now()
                        ))
                        saved_count += 1
                        print(f"💾 Сохранена исходная фраза: '{original_phrase_from_api}' (запросов: {total_count})")
                    else:
                        print(f"⏭️ Исходная фраза '{original_phrase_from_api}' уже свежая")
                else:
                    # Вставляем новую запись для исходной фразы
                    insert_original_query = """
                        INSERT INTO gen_report_context_contracts.wordstatkeyphrases 
                        (phrase, regions, devices, count, is_deleted, create_entry)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    
                    self.cursor.execute(insert_original_query, (
                        original_phrase_from_api,
                        '[213]',  # Москва
                        '["all"]',  # Все устройства
                        total_count,  # Общее количество запросов
                        False,
                        datetime.now()
                    ))
                    saved_count += 1
                    print(f"💾 Сохранена исходная фраза: '{original_phrase_from_api}' (запросов: {total_count})")
            
            # Теперь обрабатываем связанные фразы
            print(f"📝 Обрабатываем {len(phrases_data['topRequests'])} связанных фраз...")
            for phrase_item in phrases_data['topRequests']:
                phrase_text = phrase_item['phrase']
                count = phrase_item['count']
                
                print(f"🔍 Проверяем связанную фразу: '{phrase_text}' (запросов: {count})")
                
                # Проверяем, существует ли уже такая фраза
                check_query = """
                    SELECT id, count, create_entry FROM gen_report_context_contracts.wordstatkeyphrases 
                    WHERE phrase = %s AND is_deleted = false
                """
                self.cursor.execute(check_query, (phrase_text,))
                existing_record = self.cursor.fetchone()
                
                if existing_record:
                    existing_id, existing_count, create_time = existing_record
                    
                    # Проверяем свежесть фразы
                    week_ago = datetime.now() - timedelta(days=7)
                    is_fresh = create_time > week_ago
                    
                    if not is_fresh:
                        # Фраза устарела - помечаем как удаленную и создаем новую
                        phrases_to_delete.append(phrase_text)
                        print(f"🔄 Фраза '{phrase_text}' устарела, обновляем данные")
                        
                        # Вставляем новую запись
                        insert_query = """
                            INSERT INTO gen_report_context_contracts.wordstatkeyphrases 
                            (phrase, regions, devices, count, is_deleted, create_entry)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        
                        self.cursor.execute(insert_query, (
                            phrase_text,
                            '[213]',  # Москва
                            '["all"]',  # Все устройства
                            count,  # Количество запросов
                            False,
                            datetime.now()
                        ))
                        saved_count += 1
                        
                    elif count > (existing_count or 0):
                        # Фраза свежая, но количество больше - обновляем
                        update_query = """
                            UPDATE gen_report_context_contracts.wordstatkeyphrases 
                            SET count = %s
                            WHERE phrase = %s AND is_deleted = false
                        """
                        self.cursor.execute(update_query, (count, phrase_text))
                        print(f"🔄 Обновлена фраза '{phrase_text}': {existing_count} → {count}")
                        updated_count += 1
                    else:
                        print(f"⏭️ Фраза '{phrase_text}' уже существует с большим количеством: {existing_count}")
                        skipped_count += 1
                    continue
                
                # Вставляем новую фразу с количеством запросов
                insert_query = """
                    INSERT INTO gen_report_context_contracts.wordstatkeyphrases 
                    (phrase, regions, devices, count, is_deleted, create_entry)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                self.cursor.execute(insert_query, (
                    phrase_text,
                    '[213]',  # Москва
                    '["all"]',  # Все устройства
                    count,  # Количество запросов
                    False,
                    datetime.now()
                ))
                saved_count += 1
            
            # Помечаем устаревшие фразы как удаленные
            if phrases_to_delete:
                self.mark_old_phrases_as_deleted(phrases_to_delete)
            
            # Сохраняем изменения
            self.connection.commit()
            
            print(f"💾 Сохранено новых фраз в БД: {saved_count}")
            print(f"🔄 Обновлено существующих фраз: {updated_count}")
            print(f"⏭️ Пропущено (уже существуют с большим количеством): {skipped_count}")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения фраз в БД: {e}")
            self.connection.rollback()
