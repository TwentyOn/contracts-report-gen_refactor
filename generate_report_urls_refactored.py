#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для генерации URL отчетов Яндекс.Директ
Генерирует динамические URL на основе данных из БД и сохраняет результаты в MinIO
"""

import os
import json
import time
import urllib.parse
from datetime import datetime
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

from database_manager import DatabaseManager
from minio_client import MinIOClient

# Загружаем переменные окружения
load_dotenv('.env')

class ReportURLGenerator:
    """Генератор URL отчетов Яндекс.Директ"""
    
    def __init__(self):
        self.db = DatabaseManager()
        self.minio_client = MinIOClient()
        self.base_url = "https://direct.yandex.ru/registered/main.pl"
        
        # Базовые параметры URL (статичные)
        self.base_params = {
            'show_stat': '1',
            'cmd': 'showStat',
            'stat_periods': '',
            'group_by_date': 'none',
            'page_size': '10000',
            'goals': '0',
            'attribution_model': 'last_click',
            'with_nds': '1',
            'group_by': 'campaign',
            'columns': 'shows,clicks,ctr,bounce_ratio',
            'columns_positions': 'shows,clicks,ctr,bounce_ratio,eshows,ectr,sum,av_sum,avg_bid,fp_shows_avg_pos,avg_x,fp_clicks_avg_pos,avg_cpm,uniq_viewers,avg_view_freq,adepth,aconv,agoalcost,agoalnum,agoalroi,agoalcrr,agoalincome,agoals_profit,aprgoodmultigoal,aprgoodmultigoal_cpa,aprgoodmultigoal_conv_rate,video_first_quartile,video_midpoint,video_third_quartile,video_complete,video_first_quartile_rate,video_midpoint_rate,video_third_quartile_rate,video_complete_rate,cpcv,viewable_impressions_mrc,nonviewable_impressions_mrc,undetermined_impressions_mrc,measured_rate_mrc,viewable_rate_mrc',
            'group_by_positions': 'client_login,campaign_type,campaign,tags,strategy_id,adgroup,banner,banner_type,contextcond_orig,criterion_type,match_type,retargeting_coef,text_source,targettype,page_group,turbo_page_type,ssp,region,physical_region,position,click_place,banner_image_type,image_size,device_type,detailed_device_type,connection_type,gender,targeting_category,autotargeting_brand_option,prisma_income_grade,ltv_level,age,inventory_type,content_targeting,offer_attributes_name,offer_attributes_vendor,offer_attributes_category,banner_title,banner_body,banner_href,device_vendor_id,os_version'
        }
    
    def process_reports(self):
        """Основной метод обработки отчетов"""
        print("🚀 Запуск генерации URL отчетов")
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
                self.process_single_report(report)
            
            return True
            
        finally:
            self.db.disconnect()
    
    def process_single_report(self, report: Dict):
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
            
            # Получаем логин из договора
            login_yandex_direct = contract_data.get('login_yandex_direct')
            if not login_yandex_direct:
                print(f"❌ Не найден логин Яндекс.Директ в договоре {contract_data['id']}")
                return
            
            print(f"🔑 Логин Яндекс.Директ: {login_yandex_direct}")
            
            # Получаем удаленные группы из поля deleted_groups
            deleted_groups = self.get_deleted_groups(request_data)
            if deleted_groups:
                print(f"🚫 Найдено удаленных групп: {len(deleted_groups)}")
                print(f"🚫 ID удаленных групп: {deleted_groups}")
            
            # Генерируем URL отчетов
            urls_data = self.generate_report_urls(
                report, request_data, contract_data, 
                campaign_ids, start_date, end_date, login_yandex_direct, deleted_groups
            )
            
            if urls_data:
                # Сохраняем данные в MinIO
                self.save_urls_data(urls_data, report)
            else:
                print("❌ Не удалось сгенерировать URL отчетов")
                
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
    
    def generate_report_urls(self, report: Dict, request_data: Dict, contract_data: Dict,
                           campaign_ids: List[int], start_date: str, end_date: str, 
                           login_yandex_direct: str, deleted_groups: List[int]) -> Optional[Dict]:
        """Генерирует URL отчетов"""
        try:
            print("🔗 Генерация URL отчетов...")
            
            urls = []
            
            # 1. Первый тип URL - статистика по кампаниям
            campaign_url = self.generate_campaign_stats_url(
                campaign_ids, start_date, end_date, login_yandex_direct, deleted_groups
            )
            if campaign_url:
                urls.append({
                    'url': campaign_url,
                    'meta': {
                        'report_id': report['id'],
                        'request_id': request_data['id'],
                        'contract_id': contract_data['id'],
                        'login_yandex_direct': login_yandex_direct,
                        'campaign_ids': campaign_ids,
                        'campaign_count': len(campaign_ids),
                        'start_date': start_date,
                        'end_date': end_date,
                        'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'url_type': 'campaign_stats',
                        'description': 'URL для получения статистики по кампаниям'
                    }
                })
            
            # 2. Второй тип URL - статистика по группам объявлений, типам баннеров и таргетингу
            adgroup_url = self.generate_adgroup_detailed_stats_url(
                campaign_ids, start_date, end_date, login_yandex_direct, deleted_groups
            )
            if adgroup_url:
                urls.append({
                    'url': adgroup_url,
                    'meta': {
                        'report_id': report['id'],
                        'request_id': request_data['id'],
                        'contract_id': contract_data['id'],
                        'login_yandex_direct': login_yandex_direct,
                        'campaign_ids': campaign_ids,
                        'campaign_count': len(campaign_ids),
                        'start_date': start_date,
                        'end_date': end_date,
                        'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'url_type': 'adgroup_detailed_stats',
                        'description': 'URL для получения детальной статистики по группам объявлений, типам баннеров и таргетингу'
                    }
                })
            
            # 3. Третий тип URL - статистика по кампаниям с расширенными колонками
            campaign_extended_url = self.generate_campaign_extended_stats_url(
                campaign_ids, start_date, end_date, login_yandex_direct, deleted_groups
            )
            if campaign_extended_url:
                urls.append({
                    'url': campaign_extended_url,
                    'meta': {
                        'report_id': report['id'],
                        'request_id': request_data['id'],
                        'contract_id': contract_data['id'],
                        'login_yandex_direct': login_yandex_direct,
                        'campaign_ids': campaign_ids,
                        'campaign_count': len(campaign_ids),
                        'start_date': start_date,
                        'end_date': end_date,
                        'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'url_type': 'campaign_extended_stats',
                        'description': 'URL для получения статистики по кампаниям с расширенными колонками (включая цели)'
                    }
                })
            
            # 4. Четвертый тип URL - статистика по баннерам с сортировкой по кликам
            banner_stats_url = self.generate_banner_stats_url(
                campaign_ids, start_date, end_date, login_yandex_direct, deleted_groups
            )
            if banner_stats_url:
                urls.append({
                    'url': banner_stats_url,
                    'meta': {
                        'report_id': report['id'],
                        'request_id': request_data['id'],
                        'contract_id': contract_data['id'],
                        'login_yandex_direct': login_yandex_direct,
                        'campaign_ids': campaign_ids,
                        'campaign_count': len(campaign_ids),
                        'start_date': start_date,
                        'end_date': end_date,
                        'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'url_type': 'banner_stats',
                        'description': 'URL для получения статистики по баннерам с сортировкой по кликам'
                    }
                })
            
            if not urls:
                print("❌ Не удалось сгенерировать ни одного URL")
                return None
            
            # Создаем структуру данных для сохранения
            urls_data = {
                'urls': urls,
                'summary': {
                    'total_urls': len(urls),
                    'report_id': report['id'],
                    'campaign_count': len(campaign_ids),
                    'date_range': f"{start_date} - {end_date}",
                    'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
            print(f"✅ Сгенерировано URL: {len(urls)}")
            for i, url_info in enumerate(urls, 1):
                print(f"🔗 URL {i}: {url_info['url'][:100]}...")
            
            return urls_data
            
        except Exception as e:
            print(f"❌ Ошибка генерации URL: {e}")
            return None
    
    def build_url(self, params: Dict[str, str]) -> str:
        """Строит URL из параметров"""
        try:
            # Кодируем параметры
            encoded_params = []
            for key, value in params.items():
                encoded_key = urllib.parse.quote(str(key), safe='')
                encoded_value = urllib.parse.quote(str(value), safe='')
                encoded_params.append(f"{encoded_key}={encoded_value}")
            
            # Собираем URL
            url = f"{self.base_url}?{'&'.join(encoded_params)}"
            return url
            
        except Exception as e:
            print(f"❌ Ошибка построения URL: {e}")
            return ""
    
    def save_urls_data(self, urls_data: Dict, report: Dict):
        """Сохраняет данные URL в MinIO"""
        try:
            # Выводим сводку
            self.display_urls_summary(urls_data)
            
            # Сохраняем данные в MinIO
            success = self.minio_client.upload_json_data(
                urls_data,
                f"report_urls_{report['id']}.json",
                report['id']
            )
            
            if success:
                print(f"💾 Данные URL отчетов сохранены в MinIO для отчета {report['id']}")
            else:
                print(f"❌ Ошибка сохранения данных URL отчетов в MinIO")
            
        except Exception as e:
            print(f"❌ Ошибка сохранения URL отчетов: {e}")
    
    def display_urls_summary(self, urls_data: Dict):
        """Выводит краткую сводку по сгенерированным URL"""
        try:
            print(f"\n📊 Сводка по сгенерированным URL:")
            
            if not isinstance(urls_data, dict):
                print("   Ошибка: неверный формат данных")
                return
            
            summary = urls_data.get('summary', {})
            urls = urls_data.get('urls', [])
            
            print(f"   Всего URL: {summary.get('total_urls', 0)}")
            print(f"   ID отчета: {summary.get('report_id', 'N/A')}")
            print(f"   Количество кампаний: {summary.get('campaign_count', 0)}")
            print(f"   Период: {summary.get('date_range', 'N/A')}")
            print(f"   Время генерации: {summary.get('generation_timestamp', 'N/A')}")
            
            if urls:
                print(f"\n🔗 Сгенерированные URL:")
                for i, url_info in enumerate(urls, 1):
                    url = url_info.get('url', '')
                    meta = url_info.get('meta', {})
                    
                    print(f"   {i}. {meta.get('description', 'URL отчета')}")
                    print(f"      Тип: {meta.get('url_type', 'N/A')}")
                    print(f"      Логин: {meta.get('login_yandex_direct', 'N/A')}")
                    print(f"      Кампаний: {meta.get('campaign_count', 0)}")
                    print(f"      URL: {url[:80]}...")
                    
        except Exception as e:
            print(f"   ❌ Ошибка при формировании сводки: {e}")
    
    def generate_multiple_urls(self, report: Dict, request_data: Dict, contract_data: Dict,
                             campaign_ids: List[int], start_date: str, end_date: str, 
                             login_yandex_direct: str) -> Optional[Dict]:
        """Генерирует несколько URL для разных типов отчетов (для будущего расширения)"""
        try:
            print("🔗 Генерация множественных URL отчетов...")
            
            urls = []
            
            # 1. URL для статистики по кампаниям
            campaign_url = self.generate_campaign_stats_url(
                campaign_ids, start_date, end_date, login_yandex_direct
            )
            if campaign_url:
                urls.append({
                    'url': campaign_url,
                    'meta': {
                        'url_type': 'campaign_stats',
                        'description': 'URL для получения статистики по кампаниям',
                        'campaign_ids': campaign_ids,
                        'campaign_count': len(campaign_ids)
                    }
                })
            
            # 2. URL для статистики по объявлениям (пример для будущего расширения)
            # ad_url = self.generate_ad_stats_url(campaign_ids, start_date, end_date, login_yandex_direct)
            # if ad_url:
            #     urls.append({
            #         'url': ad_url,
            #         'meta': {
            #             'url_type': 'ad_stats',
            #             'description': 'URL для получения статистики по объявлениям',
            #             'campaign_ids': campaign_ids,
            #             'campaign_count': len(campaign_ids)
            #         }
            #     })
            
            if not urls:
                print("❌ Не удалось сгенерировать ни одного URL")
                return None
            
            # Создаем метаинформацию
            meta_info = {
                'report_id': report['id'],
                'request_id': request_data['id'],
                'contract_id': contract_data['id'],
                'login_yandex_direct': login_yandex_direct,
                'campaign_ids': campaign_ids,
                'campaign_count': len(campaign_ids),
                'start_date': start_date,
                'end_date': end_date,
                'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # Создаем структуру данных для сохранения
            urls_data = {
                'urls': urls,
                'meta': meta_info,
                'summary': {
                    'total_urls': len(urls),
                    'report_id': report['id'],
                    'campaign_count': len(campaign_ids),
                    'date_range': f"{start_date} - {end_date}",
                    'generation_timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
            
            print(f"✅ Сгенерировано URL: {len(urls)}")
            return urls_data
            
        except Exception as e:
            print(f"❌ Ошибка генерации множественных URL: {e}")
            return None
    
    def generate_campaign_stats_url(self, campaign_ids: List[int], start_date: str, 
                                  end_date: str, login_yandex_direct: str, deleted_groups: List[int]) -> Optional[str]:
        """Генерирует URL для статистики по кампаниям"""
        try:
            # Создаем параметры для URL
            url_params = self.base_params.copy()
            
            # Добавляем динамические параметры
            url_params['ulogin'] = login_yandex_direct
            url_params['date_from'] = start_date
            url_params['date_to'] = end_date
            
            # Добавляем ID кампаний
            campaign_ids_str = ','.join(map(str, campaign_ids))
            url_params['fl_campaign__eq[]'] = campaign_ids_str
            
            # Добавляем параметр для исключения удаленных групп
            if deleted_groups:
                deleted_groups_str = '\r\n'.join(map(str, deleted_groups))
                url_params['fl_adgroup_id__ne'] = deleted_groups_str
                print(f"🔍 Отладка: добавлен параметр fl_adgroup_id__ne = {deleted_groups_str}")
            else:
                print("🔍 Отладка: deleted_groups пустой, параметр fl_adgroup_id__ne не добавлен")
            
            # Генерируем URL
            generated_url = self.build_url(url_params)
            print(f"🔍 Отладка: сгенерированный URL: {generated_url[:200]}...")
            return generated_url
            
        except Exception as e:
            print(f"❌ Ошибка генерации URL статистики кампаний: {e}")
            return None
    
    def generate_adgroup_detailed_stats_url(self, campaign_ids: List[int], start_date: str, 
                                          end_date: str, login_yandex_direct: str, deleted_groups: List[int]) -> Optional[str]:
        """Генерирует URL для детальной статистики по группам объявлений, типам баннеров и таргетингу"""
        try:
            # Параметры для второго типа URL (из вашего примера)
            url_params = {
                'show_stat': '1',
                'cmd': 'showStat',
                'stat_periods': '',  # Будет заполнено динамически
                'ulogin': login_yandex_direct,  # Динамический
                'stat_type': 'mol',
                'group_by_date': 'none',
                'page_size': '10000',
                'date_from': start_date,  # Динамический
                'date_to': end_date,  # Динамический
                'goals': '0',
                'attribution_model': 'last_click',
                'with_nds': '1',
                'fl_campaign__eq[]': ','.join(map(str, campaign_ids)),  # Динамический
                'columns': 'clicks,sum,bounce_ratio,av_sum',
                'group_by': 'campaign_type,adgroup,banner_type,targettype',
                'columns_positions': 'clicks,sum,bounce_ratio,av_sum,ctr,eshows,shows,ectr,avg_bid,fp_shows_avg_pos,avg_x,fp_clicks_avg_pos,avg_cpm,uniq_viewers,avg_view_freq,adepth,aconv,agoalcost,agoalnum,agoalroi,agoalcrr,agoalincome,agoals_profit,aprgoodmultigoal,aprgoodmultigoal_cpa,aprgoodmultigoal_conv_rate,video_first_quartile,video_midpoint,video_third_quartile,video_complete,video_first_quartile_rate,video_midpoint_rate,video_third_quartile_rate,video_complete_rate,cpcv,viewable_impressions_mrc,nonviewable_impressions_mrc,undetermined_impressions_mrc,measured_rate_mrc,viewable_rate_mrc',
                'group_by_positions': 'campaign_type,adgroup,banner_type,targettype,campaign,tags,strategy_id,banner,contextcond_orig,criterion_type,match_type,retargeting_coef,text_source,page_group,turbo_page_type,ssp,region,physical_region,position,click_place,banner_image_type,image_size,device_type,detailed_device_type,connection_type,gender,targeting_category,autotargeting_brand_option,prisma_income_grade,ltv_level,age,inventory_type,content_targeting,offer_attributes_name,offer_attributes_vendor,offer_attributes_category,banner_title,banner_body,banner_href,device_vendor_id,os_version'
            }
            
            # Добавляем параметр для исключения удаленных групп
            if deleted_groups:
                deleted_groups_str = '\r\n'.join(map(str, deleted_groups))
                url_params['fl_adgroup_id__ne'] = deleted_groups_str
            
            # Генерируем URL
            generated_url = self.build_url(url_params)
            return generated_url
            
        except Exception as e:
            print(f"❌ Ошибка генерации URL детальной статистики: {e}")
            return None
    
    def generate_campaign_extended_stats_url(self, campaign_ids: List[int], start_date: str, 
                                           end_date: str, login_yandex_direct: str, deleted_groups: List[int]) -> Optional[str]:
        """Генерирует URL для статистики по кампаниям с расширенными колонками (включая цели)"""
        try:
            # Параметры для третьего типа URL (из вашего примера)
            url_params = {
                'show_stat': '1',
                'cmd': 'showStat',
                'stat_periods': '',  # Будет заполнено динамически
                'ulogin': login_yandex_direct,  # Динамический
                'stat_type': 'mol',
                'group_by_date': 'none',
                'page_size': '10000',
                'date_from': start_date,  # Динамический
                'date_to': end_date,  # Динамический
                'goals': '0',
                'attribution_model': 'last_click',
                'with_nds': '1',
                'fl_campaign__eq[]': ','.join(map(str, campaign_ids)),  # Динамический
                'columns': 'sum,shows,clicks,ctr,av_sum,bounce_ratio,adepth,agoalnum,agoalcost',
                'group_by': 'campaign',
                'columns_positions': 'sum,shows,clicks,ctr,av_sum,bounce_ratio,adepth,agoalnum,agoalcost,eshows,ectr,avg_bid,fp_shows_avg_pos,avg_x,fp_clicks_avg_pos,avg_cpm,uniq_viewers,avg_view_freq,aconv,agoalroi,agoalcrr,agoalincome,agoals_profit,aprgoodmultigoal,aprgoodmultigoal_cpa,aprgoodmultigoal_conv_rate,video_first_quartile,video_midpoint,video_third_quartile,video_complete,video_first_quartile_rate,video_midpoint_rate,video_third_quartile_rate,video_complete_rate,cpcv,viewable_impressions_mrc,nonviewable_impressions_mrc,undetermined_impressions_mrc,measured_rate_mrc,viewable_rate_mrc',
                'group_by_positions': 'campaign_type,adgroup,banner_type,targettype,campaign,tags,strategy_id,banner,contextcond_orig,criterion_type,match_type,retargeting_coef,text_source,page_group,turbo_page_type,ssp,region,physical_region,position,click_place,banner_image_type,image_size,device_type,detailed_device_type,connection_type,gender,targeting_category,autotargeting_brand_option,prisma_income_grade,ltv_level,age,inventory_type,content_targeting,offer_attributes_name,offer_attributes_vendor,offer_attributes_category,banner_title,banner_body,banner_href,device_vendor_id,os_version'
            }
            
            # Добавляем параметр для исключения удаленных групп
            if deleted_groups:
                deleted_groups_str = '\r\n'.join(map(str, deleted_groups))
                url_params['fl_adgroup_id__ne'] = deleted_groups_str
            
            # Генерируем URL
            generated_url = self.build_url(url_params)
            return generated_url
            
        except Exception as e:
            print(f"❌ Ошибка генерации URL расширенной статистики кампаний: {e}")
            return None
    
    def generate_banner_stats_url(self, campaign_ids: List[int], start_date: str, 
                                 end_date: str, login_yandex_direct: str, deleted_groups: List[int]) -> Optional[str]:
        """Генерирует URL для статистики по баннерам с сортировкой по кликам"""
        try:
            # Параметры для четвертого типа URL (из вашего примера)
            url_params = {
                'show_stat': '1',
                'cmd': 'showStat',
                'stat_periods': '',  # Будет заполнено динамически
                'ulogin': login_yandex_direct,  # Динамический
                'stat_type': 'mol',
                'sort': 'clicks',  # Сортировка по кликам
                'reverse': '1',  # Обратная сортировка (по убыванию)
                'group_by_date': 'none',
                'page_size': '10000',
                'date_from': start_date,  # Динамический
                'date_to': end_date,  # Динамический
                'goals': '0',
                'attribution_model': 'last_click',
                'with_nds': '1',
                'fl_campaign__eq[]': ','.join(map(str, campaign_ids)),  # Динамический
                'columns': 'clicks,av_sum,bounce_ratio',
                'group_by': 'banner',
                'columns_positions': 'clicks,av_sum,bounce_ratio,sum,ctr,eshows,shows,ectr,avg_bid,fp_shows_avg_pos,avg_x,fp_clicks_avg_pos,avg_cpm,uniq_viewers,avg_view_freq,adepth,aconv,agoalcost,agoalnum,agoalroi,agoalcrr,agoalincome,agoals_profit,aprgoodmultigoal,aprgoodmultigoal_cpa,aprgoodmultigoal_conv_rate,video_first_quartile,video_midpoint,video_third_quartile,video_complete,video_first_quartile_rate,video_midpoint_rate,video_third_quartile_rate,video_complete_rate,cpcv,viewable_impressions_mrc,nonviewable_impressions_mrc,undetermined_impressions_mrc,measured_rate_mrc,viewable_rate_mrc',
                'group_by_positions': 'campaign_type,adgroup,banner_type,targettype,campaign,tags,strategy_id,banner,contextcond_orig,criterion_type,match_type,retargeting_coef,text_source,page_group,turbo_page_type,ssp,region,physical_region,position,click_place,banner_image_type,image_size,device_type,detailed_device_type,connection_type,gender,targeting_category,autotargeting_brand_option,prisma_income_grade,ltv_level,age,inventory_type,content_targeting,offer_attributes_name,offer_attributes_vendor,offer_attributes_category,banner_title,banner_body,banner_href,device_vendor_id,os_version'
            }
            
            # Добавляем параметр для исключения удаленных групп
            if deleted_groups:
                deleted_groups_str = '\r\n'.join(map(str, deleted_groups))
                url_params['fl_adgroup_id__ne'] = deleted_groups_str
            
            # Генерируем URL
            generated_url = self.build_url(url_params)
            return generated_url
            
        except Exception as e:
            print(f"❌ Ошибка генерации URL статистики по баннерам: {e}")
            return None


def main():
    """Основная функция"""
    print("🚀 Запуск скрипта генерации URL отчетов")
    print("="*60)
    
    # Проверяем наличие необходимых переменных окружения
    required_vars = ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'S3_ENDPOINT_URL', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_BUCKET_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Отсутствуют переменные окружения: {', '.join(missing_vars)}")
        print("Проверьте файл .env")
        return
    
    # Создаем и запускаем генератор
    generator = ReportURLGenerator()
    
    try:
        success = generator.process_reports()
        if success:
            print("\n✅ Генерация URL завершена успешно")
        else:
            print("\n❌ Генерация URL завершена с ошибками")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")


if __name__ == "__main__":
    main()
