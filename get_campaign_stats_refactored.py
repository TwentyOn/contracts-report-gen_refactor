#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для получения статистики по кампаниям из API отчетов Яндекс.Директ
Использует общие модули database_manager и api_client
"""

import os
import json
import time
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any

from database_manager import DatabaseManager
from api_client import DirectAPIClient
from minio_client import MinIOClient

class CampaignStatsProcessor:
    """Обработчик для получения статистики по кампаниям"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.api_client = None
        self.minio_client = MinIOClient()
        self.current_account = None
    
    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск получения статистики по кампаниям")
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
            # Получаем данные заявки и договора
            request_data = self.db.get_request_data(report['id_requests'])
            contract_data = self.db.get_contract_data(report['id_contracts'])
            
            if not request_data or not contract_data:
                print(f"❌ Не найдены данные заявки или договора для отчета {report['id']}")
                return
            
            # Извлекаем ID кампаний
            campaign_ids = self.db.extract_campaign_ids(request_data.get('campany_yandex_direct'))
            if not campaign_ids:
                print(f"❌ Не найдены ID кампаний для отчета {report['id']}")
                return
            
            print(f"📊 Найдено кампаний: {len(campaign_ids)}")
            print(f"📊 ID кампаний: {campaign_ids}")
            
            # Получаем даты из заявки
            start_date, end_date = self.get_report_dates(request_data)
            if not start_date or not end_date:
                print(f"❌ Не найдены даты начала и окончания для отчета {report['id']}")
                return
            
            print(f"📅 Период отчета: {start_date} - {end_date}")
            
            # Получаем удаленные группы для исключения
            deleted_group_ids = self.parse_deleted_groups(request_data)
            
            # Пытаемся получить данные с разными аккаунтами
            stats_data = None
            
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
                    
                    # Получаем статистику по кампаниям
                    stats_data = self.get_campaign_stats(campaign_ids, start_date, end_date, deleted_group_ids)
                    if stats_data:
                        print("✅ Статистика по кампаниям получена успешно")
                        
                        # Получаем сводный отчет
                        summary_data = self.get_campaign_stats_summary(campaign_ids, start_date, end_date, deleted_group_ids)
                        if summary_data:
                            print("✅ Сводный отчет получен успешно")
                            # Добавляем сводные данные к основным данным
                            stats_data['summary'] = summary_data
                        else:
                            print("⚠️ Не удалось получить сводный отчет")
                        
                        self.current_account = account
                        break
                    else:
                        print("❌ Не удалось получить статистику по кампаниям")
                else:
                    print("❌ Ошибка подключения к API")
                
                # Небольшая пауза между попытками
                time.sleep(2)
            
            if stats_data:
                # Сохраняем данные
                self.save_campaign_stats(stats_data, report)
            else:
                print("❌ Не удалось получить статистику ни с одним аккаунтом")
                
        except Exception as e:
            print(f"❌ Ошибка обработки отчета: {e}")
    
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
    
    def get_campaign_stats(self, campaign_ids: List[int], start_date: str, end_date: str, deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Получает статистику по кампаниям через Reports API"""
        try:
            # Если есть удаленные группы, используем кастомный отчет с фильтрацией
            if deleted_group_ids:
                print("🔧 Используем кастомный отчет с фильтрацией по группам")
                report_data = self.api_client.create_custom_campaign_report_with_group_filter(
                    campaign_ids, start_date, end_date, deleted_group_ids
                )
            else:
                # Если нет удаленных групп, используем стандартный отчет
                report_data = self.api_client.create_campaign_performance_report(
                    campaign_ids, start_date, end_date
                )
            
            if not report_data:
                print("❌ Не удалось создать отчет")
                return None
            
            print("🔍 Отчет создан, ожидаем обработки...")
            
            # Ждем обработки отчета
            processed_data = self.wait_for_report_processing(report_data)
            
            if processed_data:
                print("✅ Отчет обработан успешно")
                return processed_data
            else:
                print("❌ Ошибка обработки отчета")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения статистики по кампаниям: {e}")
            return None
    
    def get_campaign_stats_summary(self, campaign_ids: List[int], start_date: str, end_date: str, deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Получает сводную статистику по кампаниям через Reports API"""
        try:
            # Если есть удаленные группы, используем кастомный сводный отчет с фильтрацией
            if deleted_group_ids:
                print("🔧 Используем кастомный сводный отчет с фильтрацией по группам")
                summary_data = self.api_client.create_custom_campaign_summary_report_with_group_filter(
                    campaign_ids, start_date, end_date, deleted_group_ids
                )
            else:
                # Если нет удаленных групп, используем стандартный сводный отчет
                summary_data = self.api_client.create_campaign_performance_summary_report(
                    campaign_ids, start_date, end_date
                )
            
            if not summary_data:
                print("❌ Не удалось создать сводный отчет")
                return None
            
            print("🔍 Сводный отчет создан, ожидаем обработки...")
            
            # Ждем обработки отчета
            processed_data = self.wait_for_report_processing(summary_data)
            
            if processed_data:
                print("✅ Сводный отчет обработан успешно")
                return processed_data
            else:
                print("❌ Ошибка обработки сводного отчета")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения сводной статистики по кампаниям: {e}")
            return None
    
    def wait_for_report_processing(self, report_data: Dict, max_wait_time: int = 300) -> Optional[Dict]:
        """Ждет обработки отчета и возвращает данные (теперь не нужен, так как API сам обрабатывает цикл)"""
        # Этот метод больше не нужен, так как create_campaign_performance_report
        # сам обрабатывает цикл ожидания согласно официальному примеру
        return report_data
    
    def save_campaign_stats(self, stats_data: Dict, report: Dict):
        """Сохраняет статистику кампаний в MinIO"""
        try:
            # Выводим сводку
            self.display_stats_summary(stats_data)
            
            # Сохраняем основные данные в MinIO
            success = self.minio_client.upload_campaign_stats_data(stats_data, report['id'])
            if success:
                print(f"💾 Данные статистики кампаний сохранены в MinIO для отчета {report['id']}")
                
                # Сохраняем сводные данные, если они есть
                if 'summary' in stats_data and stats_data['summary']:
                    summary_success = self.minio_client.upload_campaign_stats_summary_data(
                        stats_data['summary'], report['id']
                    )
                    if summary_success:
                        print(f"💾 Сводные данные статистики кампаний сохранены в MinIO для отчета {report['id']}")
                    else:
                        print(f"❌ Ошибка сохранения сводных данных статистики кампаний в MinIO")
            else:
                print(f"❌ Ошибка сохранения данных статистики кампаний в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения статистики кампаний: {e}")
    
    def display_stats_summary(self, stats_data: Dict):
        """Выводит краткую сводку по статистике кампаний"""
        try:
            print(f"\n📊 Сводка по статистике кампаний:")
            
            if not isinstance(stats_data, dict):
                print("   Ошибка: неверный формат данных")
                return
            
            # Проверяем структуру данных отчета
            if 'report' in stats_data:
                report_content = stats_data['report']
                report_format = stats_data.get('_meta', {}).get('format', 'TSV')
                
                if report_format == 'TSV':
                    # Обрабатываем TSV формат
                    try:
                        lines = report_content.strip().split('\n')
                        if len(lines) < 3:  # Заголовок + названия колонок + данные
                            print("   Нет данных в отчете")
                            return
                        
                        # Пропускаем заголовок отчета и названия колонок (первые 2 строки)
                        data_lines = lines[2:]
                        
                        # Удаляем последнюю строку с "Total rows: X" если есть
                        if data_lines and data_lines[-1].startswith('Total rows:'):
                            data_lines = data_lines[:-1]
                        
                        # Удаляем пустые строки
                        data_lines = [line.strip() for line in data_lines if line.strip()]
                        
                        print(f"   Найдено записей: {len(data_lines)}")
                        
                        if data_lines:
                            print(f"\n🔍 Первые 3 кампании:")
                            shown_campaigns = 0
                            total_impressions = 0
                            total_clicks = 0
                            
                            for line in data_lines:
                                if shown_campaigns >= 3:
                                    break
                                
                                # Парсим TSV строку
                                fields = line.split('\t')
                                if len(fields) >= 6: # CampaignId, CampaignName, Impressions, Clicks, Ctr, BounceRate
                                    campaign_id = fields[0] if fields[0] else 'N/A'
                                    campaign_name = fields[1] if fields[1] else 'N/A'
                                    impressions = int(fields[2]) if fields[2].isdigit() else 0
                                    clicks = int(fields[3]) if fields[3].isdigit() else 0
                                    ctr = float(fields[4]) if fields[4].replace('.', '').isdigit() else 0
                                    bounce_rate = float(fields[5]) if fields[5].replace('.', '').isdigit() else 0
                                    
                                    print(f"   {shown_campaigns + 1}. {campaign_name} (ID: {campaign_id})")
                                    print(f"      Показы: {impressions}, Клики: {clicks}")
                                    print(f"      CTR: {ctr}%, Отказы: {bounce_rate}%")
                                    
                                    total_impressions += impressions
                                    total_clicks += clicks
                                    shown_campaigns += 1
                            
                            print(f"\n📈 Общая статистика:")
                            print(f"   Всего показов: {total_impressions}")
                            print(f"   Всего кликов: {total_clicks}")
                            if total_impressions > 0:
                                overall_ctr = (total_clicks / total_impressions) * 100
                                print(f"   Общий CTR: {overall_ctr:.2f}%")
                            
                            # Подсчитываем общую стоимость и среднюю стоимость клика
                            total_cost = 0.0
                            total_avg_cpc = 0.0
                            campaigns_with_cost = 0
                            
                            for line in data_lines:
                                fields = line.split('\t')
                                if len(fields) >= 8:  # CampaignId, CampaignName, Impressions, Clicks, Ctr, BounceRate, Cost, AvgCpc
                                    cost = float(fields[6]) if fields[6].replace('.', '').isdigit() else 0.0
                                    avg_cpc = float(fields[7]) if fields[7].replace('.', '').isdigit() else 0.0
                                    total_cost += cost
                                    if avg_cpc > 0:
                                        total_avg_cpc += avg_cpc
                                        campaigns_with_cost += 1
                            
                            if total_cost > 0:
                                print(f"   Общая стоимость: {total_cost:.2f} руб.")
                            
                            if campaigns_with_cost > 0:
                                overall_avg_cpc = total_avg_cpc / campaigns_with_cost
                                print(f"   Средняя стоимость клика: {overall_avg_cpc:.2f} руб.")
                            
                            if total_clicks > 0 and total_cost > 0:
                                actual_avg_cpc = total_cost / total_clicks
                                print(f"   Фактическая средняя стоимость клика: {actual_avg_cpc:.2f} руб.")
                        else:
                            print("   Нет данных в отчете")
                            
                    except Exception as e:
                        print(f"   Ошибка обработки TSV отчета: {e}")
                        print(f"   Содержимое: {str(report_content)[:200]}...")
                        
                else:
                    # Для JSON формата пытаемся распарсить содержимое
                    try:
                        if isinstance(report_content, str):
                            report_data = json.loads(report_content)
                        else:
                            report_data = report_content
                        
                        # Проверяем структуру JSON отчета
                        if 'result' in report_data and 'rows' in report_data['result']:
                            rows = report_data['result']['rows']
                            print(f"   Найдено записей: {len(rows)}")
                            
                            if rows:
                                print(f"\n🔍 Первые 3 кампании:")
                                shown_campaigns = 0
                                total_impressions = 0
                                total_clicks = 0
                                
                                for row in rows:
                                    if shown_campaigns >= 3:
                                        break
                                    
                                    campaign_id = row.get('CampaignId', 'N/A')
                                    campaign_name = row.get('CampaignName', 'N/A')
                                    impressions = row.get('Impressions', 0)
                                    clicks = row.get('Clicks', 0)
                                    ctr = row.get('Ctr', 0)
                                    bounce_rate = row.get('BounceRate', 0)
                                    
                                    print(f"   {shown_campaigns + 1}. {campaign_name} (ID: {campaign_id})")
                                    print(f"      Показы: {impressions}, Клики: {clicks}")
                                    print(f"      CTR: {ctr}%, Отказы: {bounce_rate}%")
                                    
                                    total_impressions += impressions
                                    total_clicks += clicks
                                    shown_campaigns += 1
                                
                                print(f"\n📈 Общая статистика:")
                                print(f"   Всего показов: {total_impressions}")
                                print(f"   Всего кликов: {total_clicks}")
                                if total_impressions > 0:
                                    overall_ctr = (total_clicks / total_impressions) * 100
                                    print(f"   Общий CTR: {overall_ctr:.2f}%")
                                
                                # Подсчитываем общую стоимость и среднюю стоимость клика
                                total_cost = 0.0
                                total_avg_cpc = 0.0
                                campaigns_with_cost = 0
                                
                                for row in rows:
                                    cost = row.get('Cost', 0)
                                    avg_cpc = row.get('AvgCpc', 0)
                                    total_cost += cost
                                    if avg_cpc > 0:
                                        total_avg_cpc += avg_cpc
                                        campaigns_with_cost += 1
                                
                                if total_cost > 0:
                                    print(f"   Общая стоимость: {total_cost:.2f} руб.")
                                
                                if campaigns_with_cost > 0:
                                    overall_avg_cpc = total_avg_cpc / campaigns_with_cost
                                    print(f"   Средняя стоимость клика: {overall_avg_cpc:.2f} руб.")
                                
                                if total_clicks > 0 and total_cost > 0:
                                    actual_avg_cpc = total_cost / total_clicks
                                    print(f"   Фактическая средняя стоимость клика: {actual_avg_cpc:.2f} руб.")
                            else:
                                print("   Нет данных в отчете")
                        else:
                            print("   Ошибка: неверная структура JSON отчета")
                            print(f"   Содержимое: {str(report_content)[:200]}...")
                            
                    except json.JSONDecodeError:
                        print("   Ошибка: не удалось распарсить JSON отчет")
                        print(f"   Содержимое: {str(report_content)[:200]}...")
            else:
                print("   Ошибка: неверная структура данных отчета")
            
            # Показываем сводные данные от Яндекса, если они есть
            if 'summary' in stats_data and stats_data['summary']:
                self.display_yandex_summary(stats_data['summary'])
                    
        except Exception as e:
            print(f"   ❌ Ошибка при формировании сводки: {e}")
            print("   Проверьте формат данных в отчете")
    
    def display_yandex_summary(self, summary_data: Dict):
        """Выводит сводные данные от Яндекса"""
        try:
            print(f"\n📊 Сводная статистика от Яндекса:")
            
            if not isinstance(summary_data, dict):
                print("   Ошибка: неверный формат сводных данных")
                return
            
            # Проверяем структуру данных сводного отчета
            if 'report' in summary_data:
                report_content = summary_data['report']
                report_format = summary_data.get('_meta', {}).get('format', 'TSV')
                
                if report_format == 'TSV':
                    # Обрабатываем TSV формат сводного отчета
                    try:
                        lines = report_content.strip().split('\n')
                        if len(lines) >= 3:  # Заголовок + названия колонок + данные
                            # Пропускаем заголовок отчета и названия колонок (первые 2 строки)
                            data_lines = lines[2:]
                            
                            # Удаляем последнюю строку с "Total rows: X" если есть
                            if data_lines and data_lines[-1].startswith('Total rows:'):
                                data_lines = data_lines[:-1]
                            
                            # Удаляем пустые строки
                            data_lines = [line.strip() for line in data_lines if line.strip()]
                            
                            if data_lines:
                                # Парсим первую строку с итоговыми данными
                                fields = data_lines[0].split('\t')
                                if len(fields) >= 6:  # Impressions, Clicks, Ctr, BounceRate, Cost, AvgCpc
                                    impressions = int(fields[0]) if fields[0].isdigit() else 0
                                    clicks = int(fields[1]) if fields[1].isdigit() else 0
                                    ctr = float(fields[2]) if fields[2].replace('.', '').isdigit() else 0
                                    bounce_rate = float(fields[3]) if fields[3].replace('.', '').isdigit() else 0
                                    cost = float(fields[4]) if fields[4].replace('.', '').isdigit() else 0.0
                                    avg_cpc = float(fields[5]) if fields[5].replace('.', '').isdigit() else 0.0
                                    
                                    print(f"   📈 Итого по всем кампаниям:")
                                    print(f"      Показы: {impressions}")
                                    print(f"      Клики: {clicks}")
                                    print(f"      CTR: {ctr}%")
                                    print(f"      Отказы: {bounce_rate}%")
                                    if cost > 0:
                                        print(f"      Стоимость: {cost:.2f} руб.")
                                    if avg_cpc > 0:
                                        print(f"      Средняя стоимость клика: {avg_cpc:.2f} руб.")
                                else:
                                    print("   Нет данных в сводном отчете")
                            else:
                                print("   Нет данных в сводном отчете")
                        else:
                            print("   Нет данных в сводном отчете")
                            
                    except Exception as e:
                        print(f"   Ошибка обработки сводного TSV отчета: {e}")
                        print(f"   Содержимое: {str(report_content)[:200]}...")
                        
                else:
                    print("   Сводный отчет в неожиданном формате")
            else:
                print("   Ошибка: неверная структура сводных данных отчета")
                
        except Exception as e:
            print(f"   ❌ Ошибка при формировании сводной сводки: {e}")
            print("   Проверьте формат сводных данных в отчете")


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта получения статистики по кампаниям")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем обработчик
    processor = CampaignStatsProcessor()
    
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
