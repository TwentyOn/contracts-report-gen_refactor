#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Рефакторенная версия скрипта для получения данных из Wordstat API
Использует общие модули database_manager и api_client
"""

import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from database_manager import DatabaseManager
from api_client import WordstatAPIClient

class WordstatProcessor:
    """Обработчик данных Wordstat"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.results_dir = "Результаты"
        self.accounts = []
        self.current_account_index = 0
    
    def process_wordstat_data(self):
        """Основной метод обработки Wordstat данных"""
        print("🚀 Запуск скрипта для обработки ключевых фраз через Wordstat API")
        print("=" * 70)
        
        # Подключаемся к БД
        if not self.db.connect():
            return False
        
        try:
            # Загружаем аккаунты
            self.accounts = self.db.get_wordstat_accounts()
            if not self.accounts:
                print("❌ Не найдено ни одного аккаунта Wordstat API")
                return False
            
            print(f"🔄 Загружено {len(self.accounts)} аккаунтов")
            
            # Обрабатываем все ключевые фразы
            result = self.process_all_keywords()
            
            # Вывод итоговой статистики
            if result['success']:
                print("\n" + "=" * 70)
                print("📊 ИТОГОВАЯ СТАТИСТИКА")
                print("=" * 70)
                print(f"📝 Всего ключевых фраз: {result['total_keywords']}")
                print(f"✅ Успешно обработано: {result['processed']}")
                print(f"⏭️ Пропущено (уже свежие): {result['skipped']}")
                print(f"❌ Ошибок: {result['total_keywords'] - result['processed'] - result['skipped']}")
                
                # Сохраняем итоговый отчет
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_file = f"{self.results_dir}/wordstat_processing_report_{timestamp}.json"
                
                with open(report_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                
                print(f"📄 Отчет сохранен: {report_file}")
            else:
                print("❌ Обработка не удалась")
                print(f"💥 Ошибка: {result['error']}")
            
            return result['success']
            
        finally:
            self.db.disconnect()
    
    def process_all_keywords(self) -> Dict:
        """Обрабатывает все ключевые фразы из файла"""
        try:
            # Загружаем ключевые фразы из файла
            keywords = self.db.load_keywords_from_file()
            if not keywords:
                return {
                    'success': False,
                    'error': 'Не найдено ключевых фраз для обработки'
                }
            
            total_processed = 0
            total_skipped = 0
            results = []
            
            # Выводим массив уникальных фраз
            print(f"\n📋 Список уникальных фраз:")
            for i, keyword in enumerate(keywords, 1):
                print(f"  {i:2d}. {keyword}")
            
            print(f"\n🔄 Начинаем обработку {len(keywords)} ключевых фраз")
            print("=" * 60)
            
            for i, keyword in enumerate(keywords, 1):
                print(f"\n📝 Обработка фразы {i}/{len(keywords)}: '{keyword}'")
                print("-" * 40)
                
                # Проверяем, есть ли свежая исходная фраза в БД
                print(f"🔍 Проверяем свежесть фразы '{keyword}' в БД...")
                is_fresh = self.db.check_phrase_freshness(keyword)
                
                if is_fresh:
                    print(f"✅ Фраза '{keyword}' СВЕЖАЯ - пропускаем API запрос")
                    total_skipped += 1
                    continue
                else:
                    print(f"🔄 Фраза '{keyword}' НЕ СВЕЖАЯ или НЕ НАЙДЕНА - делаем API запрос")
                
                
                # Отправляем запрос к API
                result = self.test_with_account_rotation(keyword)
                
                if result['success']:
                    total_processed += 1
                    results.append({
                        'keyword': keyword,
                        'status': 'success',
                        'phrases_count': len(result['data'].get('topRequests', []))
                    })
                    print(f"✅ Фраза '{keyword}' обработана успешно")
                else:
                    results.append({
                        'keyword': keyword,
                        'status': 'error',
                        'error': result['error']
                    })
                    print(f"❌ Ошибка обработки фразы '{keyword}': {result['error']}")
                
                # Небольшая пауза между запросами
                time.sleep(2)
            
            return {
                'success': True,
                'total_keywords': len(keywords),
                'processed': total_processed,
                'skipped': total_skipped,
                'results': results
            }
            
        except Exception as e:
            print(f"❌ Критическая ошибка обработки ключевых фраз: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_current_account(self) -> Dict:
        """Получает текущий аккаунт"""
        if not self.accounts:
            raise Exception("Аккаунты не загружены")
        return self.accounts[self.current_account_index]
    
    def switch_to_next_account(self):
        """Переключается на следующий аккаунт"""
        if self.current_account_index < len(self.accounts) - 1:
            self.current_account_index += 1
            print(f"🔄 Переключение на аккаунт {self.current_account_index + 1}")
        else:
            raise Exception("Все аккаунты исчерпаны")
    
    def test_wordstat_api(self, phrase: str) -> Dict:
        """Тестирует API Wordstat с заданной фразой"""
        account = self.get_current_account()
        
        # Создаем Wordstat клиент
        wordstat_client = WordstatAPIClient(
            account['wordstat_token'],
            account['wordstat_login']
        )
        
        try:
            print(f"🔍 Тестирование API с фразой: '{phrase}'")
            print(f"📧 Используется аккаунт: {account['wordstat_login']}")
            
            # Получаем данные
            result = wordstat_client.get_top_requests(phrase)
            
            if result:
                print("✅ API запрос выполнен успешно")
                
                # Получаем информацию о квотах пользователя
                user_info = wordstat_client.get_user_info()
                if user_info:
                    print(f"📊 Квоты пользователя: {user_info}")
                    result['userInfo'] = user_info
                
                # Сохраняем фразы в БД
                self.db.save_phrases_to_db(result, phrase)
                
                return {
                    'success': True,
                    'data': result,
                    'account_used': account['wordstat_login'],
                    'phrase': phrase
                }
            else:
                print(f"❌ Ошибка API")
                return {
                    'success': False,
                    'error': "API вернул пустой результат",
                    'account_used': account['wordstat_login'],
                    'phrase': phrase
                }
                
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
            return {
                'success': False,
                'error': str(e),
                'account_used': account['wordstat_login'],
                'phrase': phrase
            }
    
    def test_with_account_rotation(self, phrase: str) -> Dict:
        """Тестирует API с автоматическим переключением аккаунтов при ошибках"""
        max_attempts = len(self.accounts)
        attempt = 0
        
        while attempt < max_attempts:
            try:
                result = self.test_wordstat_api(phrase)
                
                if result['success']:
                    return result
                else:
                    print(f"⚠️ Ошибка с аккаунтом {result['account_used']}: {result['error']}")
                    if attempt < max_attempts - 1:
                        self.switch_to_next_account()
                        time.sleep(2)  # Небольшая пауза между попытками
                    attempt += 1
                    
            except Exception as e:
                print(f"❌ Критическая ошибка: {e}")
                if attempt < max_attempts - 1:
                    self.switch_to_next_account()
                    time.sleep(2)
                attempt += 1
        
        return {
            'success': False,
            'error': 'Все аккаунты исчерпаны',
            'phrase': phrase
        }


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта для обработки ключевых фраз через Wordstat API")
    print("=" * 70)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = WordstatProcessor()
    
    try:
        success = processor.process_wordstat_data()
        if success:
            print("\n✅ Обработка завершена успешно")
        else:
            print("\n❌ Обработка завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
    finally:
        print("\n🏁 Скрипт завершен")


if __name__ == "__main__":
    main()
