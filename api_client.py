#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Общий модуль для работы с API Яндекс.Директ
Содержит классы для работы с различными API методами
"""

import os
import json
import time
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

class DirectAPIClient:
    """Клиент для работы с API Яндекс.Директ"""
    
    def __init__(self, token: str, client_login: str = None):
        self.token = token
        self.client_login = client_login
        self.base_url = 'https://api.direct.yandex.com/json/v5'
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Accept-Language': 'ru',
            'Content-Type': 'application/json'
        }
        # Используем Client-Login только если это действительно логин клиента (не Client ID)
        if self.client_login:
            # Если это длинный Client ID (обычно 32+ символа), не используем его как логин
            if len(self.client_login) > 20 and self.client_login.replace('-', '').replace('_', '').isalnum():
                print(f"⚠️ Пропускаем Client-Login '{self.client_login}' - похоже на Client ID, а не логин")
            else:
                self.headers['Client-Login'] = self.client_login
                print(f"✅ Используем Client-Login: {self.client_login}")
        else:
            print(f"ℹ️ Client-Login не указан - запросы выполняются от имени владельца токена")
    
    def test_connection(self) -> bool:
        """Тестирует подключение к API"""
        try:
            # Простой запрос для проверки доступности API
            method = 'campaigns'
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {},
                    "FieldNames": ["Id", "Name"]
                }
            }
            
            response = requests.post(
                f"{self.base_url}/{method}",
                headers=self.headers,
                json=params,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if 'error' in result:
                    error = result['error']
                    print(f"❌ Ошибка API: {error.get('error_string', 'Неизвестная ошибка')}")
                    print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                    return False
                return True
            else:
                print(f"❌ Ошибка HTTP: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Ошибка тестирования API: {e}")
            return False
    
    def get_ads_by_campaigns(self, campaign_ids: List[int]) -> Optional[Dict]:
        """Получает объявления по ID кампаний"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        # Разбиваем список кампаний на группы по 3 кампании
        batch_size = 3
        campaign_batches = [campaign_ids[i:i + batch_size] for i in range(0, len(campaign_ids), batch_size)]
        
        print(f"\n📦 Разбиваем запрос на {len(campaign_batches)} частей")
        
        method = 'ads'
        field_names = [
            "Id",
            "Type",
            "AdGroupId",
            "CampaignId",
            "State",
            "Status"
        ]
        text_ad_fields = [
            "Title",
            "Title2",
            "Href",
            "Text",
            "SitelinkSetId",
            "VCardId",
            "AdImageHash",
            "DisplayUrlPath",
            "AdExtensions"
        ]
        
        all_ads = []
        
        for batch_index, batch in enumerate(campaign_batches, 1):
            print(f"\n📋 Обработка части {batch_index}/{len(campaign_batches)}: кампании {batch}")
            
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {
                        "CampaignIds": batch
                    },
                    "FieldNames": field_names,
                    "TextAdFieldNames": text_ad_fields
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/{method}",
                    headers=self.headers,
                    json=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'error' in result:
                        error = result['error']
                        print(f"❌ Ошибка API: {error.get('error_string', 'Неизвестная ошибка')}")
                        print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                        continue
                    
                    if 'result' in result and 'Ads' in result['result']:
                        ads = result['result']['Ads']
                        print(f"✅ Получено объявлений: {len(ads)}")
                        all_ads.extend(ads)
                    else:
                        print("⚠️ Объявления не найдены")
                        
                else:
                    print(f"❌ Ошибка HTTP: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Ошибка запроса: {e}")
                
            # Небольшая пауза между запросами
            if batch_index < len(campaign_batches):
                time.sleep(1)
        
        if all_ads:
            print(f"\n📊 Всего получено объявлений: {len(all_ads)}")
            
            # Возвращаем результат в старом формате для совместимости
            result = {
                'result': {
                    'Ads': all_ads
                },
                '_meta': {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'api_method': 'ads.get',
                    'api_version': 'v5'
                }
            }
            return result
        else:
            print("❌ Объявления не найдены")
            return None
    
    def get_adgroups_by_campaigns(self, campaign_ids: List[int]) -> Optional[Dict]:
        """Получает группы объявлений по ID кампаний"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        # Разбиваем список кампаний на группы по 3 кампании
        batch_size = 3
        campaign_batches = [campaign_ids[i:i + batch_size] for i in range(0, len(campaign_ids), batch_size)]
        
        print(f"\n📦 Разбиваем запрос на {len(campaign_batches)} частей")
        
        method = 'adgroups'
        field_names = [
            "Id",
            "Name",
            "CampaignId",
            "Type",
            "Status",
            "NegativeKeywords",
            "TrackingParams"
        ]
        
        all_adgroups = []
        
        for batch_index, batch in enumerate(campaign_batches, 1):
            print(f"\n📋 Обработка части {batch_index}/{len(campaign_batches)}: кампании {batch}")
            
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {
                        "CampaignIds": batch
                    },
                    "FieldNames": field_names
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/{method}",
                    headers=self.headers,
                    json=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'error' in result:
                        error = result['error']
                        print(f"❌ Ошибка API: {error.get('error_string', 'Неизвестная ошибка')}")
                        print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                        continue
                    
                    if 'result' in result and 'AdGroups' in result['result']:
                        adgroups = result['result']['AdGroups']
                        print(f"✅ Получено групп объявлений: {len(adgroups)}")
                        all_adgroups.extend(adgroups)
                    else:
                        print("⚠️ Группы объявлений не найдены")
                        
                else:
                    print(f"❌ Ошибка HTTP: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Ошибка запроса: {e}")
                
            # Небольшая пауза между запросами
            if batch_index < len(campaign_batches):
                time.sleep(1)
        
        if all_adgroups:
            print(f"\n📊 Всего получено групп объявлений: {len(all_adgroups)}")
            
            # Возвращаем результат в старом формате для совместимости
            result = {
                'result': {
                    'AdGroups': all_adgroups
                },
                '_meta': {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'api_method': 'adgroups.get',
                    'api_version': 'v5'
                }
            }
            return result
        else:
            print("❌ Группы объявлений не найдены")
            return None
    
    def get_campaigns_data(self, campaign_ids: List[int]) -> Optional[Dict]:
        """Получает данные кампаний по ID"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        # Разбиваем список кампаний на группы по 3 кампании
        batch_size = 3
        campaign_batches = [campaign_ids[i:i + batch_size] for i in range(0, len(campaign_ids), batch_size)]
        
        print(f"\n📦 Разбиваем запрос на {len(campaign_batches)} частей")
        
        method = 'campaigns'
        field_names = [
            "Id",
            "Name",
            "Type",
            "Status",
            "State",
            "StatusPayment",
            "StatusClarification",
            "Currency",
            "DailyBudget",
            "Notification",
            "TimeTargeting",
            "TimeZone",
            "StartDate",
            "EndDate",
            "NegativeKeywords",
            "BlockedIps",
            "ExcludedSites",
            "TextCampaign",
            "MobileAppCampaign",
            "DynamicTextCampaign",
            "CpmBannerCampaign"
        ]
        
        all_campaigns = []
        
        for batch_index, batch in enumerate(campaign_batches, 1):
            print(f"\n📋 Обработка части {batch_index}/{len(campaign_batches)}: кампании {batch}")
            
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {
                        "Ids": batch
                    },
                    "FieldNames": field_names
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/{method}",
                    headers=self.headers,
                    json=params,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'error' in result:
                        error = result['error']
                        print(f"❌ Ошибка API: {error.get('error_string', 'Неизвестная ошибка')}")
                        print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                        continue
                    
                    if 'result' in result and 'Campaigns' in result['result']:
                        campaigns = result['result']['Campaigns']
                        print(f"✅ Получено кампаний: {len(campaigns)}")
                        all_campaigns.extend(campaigns)
                    else:
                        print("⚠️ Кампании не найдены")
                        
                else:
                    print(f"❌ Ошибка HTTP: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Ошибка запроса: {e}")
                
            # Небольшая пауза между запросами
            if batch_index < len(campaign_batches):
                time.sleep(1)
        
        if all_campaigns:
            print(f"\n📊 Всего получено кампаний: {len(all_campaigns)}")
            
            # Возвращаем результат в старом формате для совместимости
            result = {
                'result': {
                    'Campaigns': all_campaigns
                },
                '_meta': {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'api_method': 'campaigns.get',
                    'api_version': 'v5'
                }
            }
            return result
        else:
            print("❌ Кампании не найдены")
            return None
    
    def get_extensions_and_sitelinks(self, campaign_ids: List[int]) -> Optional[Dict]:
        """Получает расширения и быстрые ссылки по ID кампаний"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        # Разбиваем список кампаний на группы по 3 кампании
        batch_size = 3
        campaign_batches = [campaign_ids[i:i + batch_size] for i in range(0, len(campaign_ids), batch_size)]
        
        print(f"\n📦 Разбиваем запрос на {len(campaign_batches)} частей")
        
        all_extensions = []
        all_sitelinks = []
        
        for batch_index, batch in enumerate(campaign_batches, 1):
            print(f"\n📋 Обработка части {batch_index}/{len(campaign_batches)}: кампании {batch}")
            
            # Получаем расширения
            extensions_result = self._get_extensions(batch)
            if extensions_result:
                all_extensions.extend(extensions_result)
            
            # Получаем быстрые ссылки
            sitelinks_result = self._get_sitelinks(batch)
            if sitelinks_result:
                all_sitelinks.extend(sitelinks_result)
            
            # Небольшая пауза между запросами
            if batch_index < len(campaign_batches):
                time.sleep(1)
        
        if all_extensions or all_sitelinks:
            print(f"\n📊 Всего получено расширений: {len(all_extensions)}")
            print(f"📊 Всего получено быстрых ссылок: {len(all_sitelinks)}")
            
            # Возвращаем результат в старом формате для совместимости
            result = {
                'result': {
                    'Extensions': all_extensions,
                    'Sitelinks': all_sitelinks
                },
                '_meta': {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'api_method': 'extensions.get + sitelinks.get',
                    'api_version': 'v5'
                }
            }
            return result
        else:
            print("❌ Расширения и быстрые ссылки не найдены")
            return None
    
    def _get_extensions(self, campaign_ids: List[int]) -> List[Dict]:
        """Получает расширения для кампаний"""
        method = 'extensions'
        field_names = [
            "Id",
            "Type",
            "State",
            "Status",
            "CampaignIds",
            "Associated",
            "Callout",
            "CalloutText",
            "CalloutTexts",
            "CallTrackingSettings",
            "CalloutExtension"
        ]
        
        params = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": campaign_ids
                },
                "FieldNames": field_names
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{method}",
                headers=self.headers,
                json=params,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if 'error' in result:
                    error = result['error']
                    print(f"❌ Ошибка API расширений: {error.get('error_string', 'Неизвестная ошибка')}")
                    return []
                
                if 'result' in result and 'Extensions' in result['result']:
                    extensions = result['result']['Extensions']
                    print(f"✅ Получено расширений: {len(extensions)}")
                    return extensions
                else:
                    print("⚠️ Расширения не найдены")
                    return []
                    
            else:
                print(f"❌ Ошибка HTTP расширений: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Ошибка запроса расширений: {e}")
            return []
    
    def _get_sitelinks(self, campaign_ids: List[int]) -> List[Dict]:
        """Получает быстрые ссылки для кампаний"""
        method = 'sitelinks'
        field_names = [
            "Id",
            "CampaignId",
            "Title",
            "Href",
            "Description",
            "TurboPageId"
        ]
        
        params = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "CampaignIds": campaign_ids
                },
                "FieldNames": field_names
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{method}",
                headers=self.headers,
                json=params,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if 'error' in result:
                    error = result['error']
                    print(f"❌ Ошибка API быстрых ссылок: {error.get('error_string', 'Неизвестная ошибка')}")
                    return []
                
                if 'result' in result and 'Sitelinks' in result['result']:
                    sitelinks = result['result']['Sitelinks']
                    print(f"✅ Получено быстрых ссылок: {len(sitelinks)}")
                    return sitelinks
                else:
                    print("⚠️ Быстрые ссылки не найдены")
                    return []
                    
            else:
                print(f"❌ Ошибка HTTP быстрых ссылок: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Ошибка запроса быстрых ссылок: {e}")
            return []
    
    def get_keywords_traffic_forecast(self, keywords: List[str], geo_id: int = 213) -> Optional[Dict]:
        """Получает прогноз трафика по ключевым словам через Wordstat API"""
        if not keywords:
            print("⚠️ Список ключевых слов пуст")
            return None
        
        print(f"🔍 Получение прогноза трафика для {len(keywords)} ключевых слов")
        print(f"🌍 Регион: {geo_id}")
        
        # Разбиваем ключевые слова на группы по 10 слов
        batch_size = 10
        keyword_batches = [keywords[i:i + batch_size] for i in range(0, len(keywords), batch_size)]
        
        print(f"\n📦 Разбиваем запрос на {len(keyword_batches)} частей")
        
        all_forecasts = []
        
        for batch_index, batch in enumerate(keyword_batches, 1):
            print(f"\n📋 Обработка части {batch_index}/{len(keyword_batches)}: {len(batch)} слов")
            
            # Формируем запрос для Wordstat API
            params = {
                "method": "CreateNewWordstatReport",
                "params": {
                    "SelectionCriteria": {
                        "Filter": [
                            {
                                "Field": "Query",
                                "Operator": "IN",
                                "Values": batch
                            }
                        ],
                        "DateFrom": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
                        "DateTo": datetime.now().strftime("%Y-%m-%d")
                    },
                    "FieldNames": [
                        "Query",
                        "Impressions",
                        "Clicks",
                        "Ctr",
                        "BounceRate"
                    ],
                    "OrderBy": [
                        {
                            "Field": "Impressions",
                            "SortOrder": "DESCENDING"
                        }
                    ],
                    "ReportName": f"Traffic Forecast Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                    "ReportType": "SEARCH_QUERY_PERFORMANCE_REPORT",
                    "DateRangeType": "LAST_30_DAYS",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                    "IncludeDiscount": "NO"
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/wordstat",
                    headers=self.headers,
                    json=params,
                    timeout=60
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'error' in result:
                        error = result['error']
                        print(f"❌ Ошибка Wordstat API: {error.get('error_string', 'Неизвестная ошибка')}")
                        print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                        continue
                    
                    if 'result' in result and 'Report' in result['result']:
                        report_data = result['result']['Report']
                        print(f"✅ Получен отчет по {len(batch)} ключевым словам")
                        
                        # Парсим TSV данные отчета
                        if 'Data' in report_data:
                            tsv_data = report_data['Data']
                            forecasts = self._parse_wordstat_tsv(tsv_data)
                            all_forecasts.extend(forecasts)
                    else:
                        print("⚠️ Отчет не найден")
                        
                else:
                    print(f"❌ Ошибка HTTP: {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Ошибка запроса: {e}")
                
            # Небольшая пауза между запросами
            if batch_index < len(keyword_batches):
                time.sleep(2)
        
        if all_forecasts:
            print(f"\n📊 Всего получено прогнозов: {len(all_forecasts)}")
            
            # Возвращаем результат в старом формате для совместимости
            result = {
                'result': {
                    'KeywordsTrafficForecast': all_forecasts
                },
                '_meta': {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'api_method': 'wordstat.CreateNewWordstatReport',
                    'api_version': 'v5'
                }
            }
            return result
        else:
            print("❌ Прогнозы не найдены")
            return None
    
    def _parse_wordstat_tsv(self, tsv_data: str) -> List[Dict]:
        """Парсит TSV данные отчета Wordstat"""
        try:
            lines = tsv_data.strip().split('\n')
            
            # Пропускаем заголовок отчета и названия колонок (первые 2 строки)
            data_lines = lines[2:]
            
            # Удаляем последнюю строку с "Total rows: X" если есть
            if data_lines and data_lines[-1].startswith('Total rows:'):
                data_lines = data_lines[:-1]
            
            # Удаляем пустые строки
            data_lines = [line.strip() for line in data_lines if line.strip()]
            
            forecasts = []
            for line in data_lines:
                fields = line.split('\t')
                if len(fields) >= 5:  # Query, Impressions, Clicks, Ctr, BounceRate
                    forecast = {
                        'Query': fields[0] if fields[0] else '',
                        'Impressions': int(fields[1]) if fields[1].isdigit() else 0,
                        'Clicks': int(fields[2]) if fields[2].isdigit() else 0,
                        'Ctr': float(fields[3]) if fields[3].replace('.', '').isdigit() else 0.0,
                        'BounceRate': float(fields[4]) if fields[4].replace('.', '').isdigit() else 0.0
                    }
                    forecasts.append(forecast)
            
            return forecasts
            
        except Exception as e:
            print(f"❌ Ошибка парсинга TSV данных Wordstat: {e}")
            return []
    
    def create_campaign_performance_report(self, campaign_ids: List[int], start_date: str, end_date: str) -> Optional[Dict]:
        """Создает отчет по производительности кампаний согласно официальному примеру"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Создание отчета по кампаниям: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем параметры отчета согласно официальному примеру
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": [
                        {
                            "Field": "CampaignId",
                            "Operator": "IN",
                            "Values": campaign_ids
                        }
                    ],
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "CampaignId",
                    "CampaignName",
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "BounceRate",
                    "Cost",
                    "AvgCpc"
                ],
                "ReportName": f"Campaign Performance Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body,
                    timeout=60
                )
                
                # Устанавливаем кодировку UTF-8 для корректного отображения русских символов
                response.encoding = 'utf-8'
                
                if response.status_code == 400:
                    print("❌ Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 200:
                    print("✅ Отчет создан успешно")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    
                    # Возвращаем данные отчета
                    result = {
                        'report': response.text,  # Содержимое отчета в TSV формате
                        'status': 'completed',
                        '_meta': {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'api_method': 'reports.post',
                            'api_version': 'v5',
                            'request_id': response.headers.get('RequestId', 'N/A'),
                            'format': 'TSV'
                        }
                    }
                    return result
                    
                elif response.status_code == 201:
                    print("⏳ Отчет поставлен в очередь в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 202:
                    print("⏳ Отчет формируется в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 500:
                    print("❌ При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 502:
                    print("❌ Время формирования отчета превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                else:
                    print("❌ Произошла непредвиденная ошибка")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
            except requests.exceptions.ConnectionError:
                print("❌ Произошла ошибка соединения с сервером API")
                return None
                
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def get_report_status(self, report_id: str) -> Optional[Dict]:
        """Получает статус отчета"""
        if not report_id:
            print("⚠️ ID отчета не указан")
            return None
        
        try:
            print(f"🔍 Проверка статуса отчета: {report_id}")
            
            # Для отчетов используем POST запрос как и для других методов
            params = {
                "params": {
                    "ReportId": report_id
                }
            }
            
            response = requests.post(
                f"{self.base_url}/reports",
                headers=self.headers,
                json=params,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                
                if 'error' in result:
                    error = result['error']
                    print(f"❌ Ошибка API: {error.get('error_string', 'Неизвестная ошибка')}")
                    print(f"Код ошибки: {error.get('error_code', 'N/A')}")
                    return None
                
                return result
            else:
                print(f"❌ Ошибка HTTP: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения статуса отчета: {e}")
            return None
    
    def get_image_hashes_from_report(self, campaign_ids: List[int], start_date: str, end_date: str) -> Optional[Dict]:
        """Получает хеши изображений из отчета по кампаниям"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Получение хешей изображений для кампаний: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем параметры отчета для получения хешей изображений
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": [
                        {
                            "Field": "CampaignId",
                            "Operator": "IN",
                            "Values": campaign_ids
                        }
                    ],
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "CampaignId",
                    "CampaignName",
                    "AdGroupId",
                    "AdGroupName",
                    "AdId",
                    "AdName",
                    "AdImageHash",
                    "Impressions",
                    "Clicks"
                ],
                "ReportName": f"Image Hashes Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "AD_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body,
                    timeout=60
                )
                
                # Устанавливаем кодировку UTF-8 для корректного отображения русских символов
                response.encoding = 'utf-8'
                
                if response.status_code == 400:
                    print("❌ Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 200:
                    print("✅ Отчет с хешами изображений создан успешно")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    
                    # Возвращаем данные отчета
                    result = {
                        'report': response.text,  # Содержимое отчета в TSV формате
                        'status': 'completed',
                        '_meta': {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'api_method': 'reports.post',
                            'api_version': 'v5',
                            'request_id': response.headers.get('RequestId', 'N/A'),
                            'format': 'TSV',
                            'type': 'image_hashes'
                        }
                    }
                    return result
                    
                elif response.status_code == 201:
                    print("⏳ Отчет с хешами изображений поставлен в очередь в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 202:
                    print("⏳ Отчет с хешами изображений формируется в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 500:
                    print("❌ При формировании отчета с хешами изображений произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 502:
                    print("❌ Время формирования отчета с хешами изображений превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                else:
                    print("❌ Произошла непредвиденная ошибка")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
            except requests.exceptions.ConnectionError:
                print("❌ Произошла ошибка соединения с сервером API")
                return None
                
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def create_custom_campaign_report_with_group_filter(self, campaign_ids: List[int], start_date: str, end_date: str, deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Создает кастомный отчет по кампаниям с возможностью фильтрации по группам"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Создание кастомного отчета по кампаниям: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем фильтры
        filters = [
            {
                "Field": "CampaignId",
                "Operator": "IN",
                "Values": campaign_ids
            }
        ]
        
        # Добавляем фильтр для исключения удаленных групп, если они есть
        if deleted_group_ids:
            filters.append({
                "Field": "AdGroupId",
                "Operator": "NOT_IN",
                "Values": deleted_group_ids
            })
            print(f"🚫 Исключаем {len(deleted_group_ids)} групп из кастомного отчета")
        
        # Формируем параметры кастомного отчета с группировкой по кампаниям
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": filters,
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "CampaignId",
                    "CampaignName",
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "BounceRate",
                    "Cost",
                    "AvgCpc"
                ],
                "ReportName": f"Custom Campaign Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body
                )
                
                if response.status_code == 200:
                    # Проверяем Content-Type ответа
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if 'application/json' in content_type:
                        # Если это JSON ответ
                        try:
                            result = response.json()
                            if 'result' in result:
                                print("✅ Кастомный отчет создан успешно")
                                return {
                                    'report': result['result'],
                                    '_meta': {
                                        'format': 'TSV',
                                        'report_type': 'CUSTOM_REPORT',
                                        'campaign_ids': campaign_ids,
                                        'deleted_group_ids': deleted_group_ids
                                    }
                                }
                            else:
                                print(f"❌ Ошибка в ответе API: {result}")
                                return None
                        except json.JSONDecodeError as e:
                            print(f"❌ Ошибка парсинга JSON: {e}")
                            return None
                    else:
                        # Если это TSV данные (обычный случай для отчетов)
                        print("✅ Кастомный отчет создан успешно")
                        return {
                            'report': response.text,
                            '_meta': {
                                'format': 'TSV',
                                'report_type': 'CUSTOM_REPORT',
                                'campaign_ids': campaign_ids,
                                'deleted_group_ids': deleted_group_ids
                            }
                        }
                elif response.status_code == 201:
                    print("⏳ Кастомный отчет поставлен в очередь в режиме офлайн")
                    print("🔄 Повторная отправка запроса через 1 секунд")
                    time.sleep(1)
                    continue
                elif response.status_code == 202:
                    print("⏳ Кастомный отчет формируется в режиме офлайн")
                    print("🔄 Повторная отправка запроса через 10 секунд")
                    time.sleep(10)
                    continue
                else:
                    print(f"❌ Ошибка HTTP {response.status_code}")
                    try:
                        error_data = response.json()
                        print(f"JSON-код ответа сервера: {error_data}")
                    except:
                        print(f"Текст ответа: {response.text}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ Ошибка запроса: {e}")
                return None
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def create_custom_campaign_summary_report_with_group_filter(self, campaign_ids: List[int], start_date: str, end_date: str, deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Создает кастомный сводный отчет по кампаниям с возможностью фильтрации по группам"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Создание кастомного сводного отчета по кампаниям: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем фильтры
        filters = [
            {
                "Field": "CampaignId",
                "Operator": "IN",
                "Values": campaign_ids
            }
        ]
        
        # Добавляем фильтр для исключения удаленных групп, если они есть
        if deleted_group_ids:
            filters.append({
                "Field": "AdGroupId",
                "Operator": "NOT_IN",
                "Values": deleted_group_ids
            })
            print(f"🚫 Исключаем {len(deleted_group_ids)} групп из кастомного сводного отчета")
        
        # Формируем параметры кастомного сводного отчета БЕЗ группировки по кампаниям
        # Это даст нам одну агрегированную строку по всем кампаниям
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": filters,
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "BounceRate",
                    "Cost",
                    "AvgCpc"
                ],
                "ReportName": f"Custom Campaign Summary Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body
                )
                
                if response.status_code == 200:
                    # Проверяем Content-Type ответа
                    content_type = response.headers.get('content-type', '').lower()
                    
                    if 'application/json' in content_type:
                        # Если это JSON ответ
                        try:
                            result = response.json()
                            if 'result' in result:
                                print("✅ Кастомный сводный отчет создан успешно")
                                return {
                                    'report': result['result'],
                                    '_meta': {
                                        'format': 'TSV',
                                        'report_type': 'CUSTOM_REPORT',
                                        'campaign_ids': campaign_ids,
                                        'deleted_group_ids': deleted_group_ids
                                    }
                                }
                            else:
                                print(f"❌ Ошибка в ответе API: {result}")
                                return None
                        except json.JSONDecodeError as e:
                            print(f"❌ Ошибка парсинга JSON: {e}")
                            return None
                    else:
                        # Если это TSV данные (обычный случай для отчетов)
                        print("✅ Кастомный сводный отчет создан успешно")
                        return {
                            'report': response.text,
                            '_meta': {
                                'format': 'TSV',
                                'report_type': 'CUSTOM_REPORT',
                                'campaign_ids': campaign_ids,
                                'deleted_group_ids': deleted_group_ids
                            }
                        }
                elif response.status_code == 201:
                    print("⏳ Кастомный сводный отчет поставлен в очередь в режиме офлайн")
                    print("🔄 Повторная отправка запроса через 1 секунд")
                    time.sleep(1)
                    continue
                elif response.status_code == 202:
                    print("⏳ Кастомный сводный отчет формируется в режиме офлайн")
                    print("🔄 Повторная отправка запроса через 10 секунд")
                    time.sleep(10)
                    continue
                else:
                    print(f"❌ Ошибка HTTP {response.status_code}")
                    try:
                        error_data = response.json()
                        print(f"JSON-код ответа сервера: {error_data}")
                    except:
                        print(f"Текст ответа: {response.text}")
                    return None
                    
            except requests.exceptions.RequestException as e:
                print(f"❌ Ошибка запроса: {e}")
                return None
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def create_campaign_performance_summary_report(self, campaign_ids: List[int], start_date: str, end_date: str) -> Optional[Dict]:
        """Создает сводный отчет по производительности кампаний без группировки"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Создание сводного отчета по кампаниям: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем параметры сводного отчета по выбранным кампаниям с использованием CUSTOM_REPORT
        # Без полей группировки - должна вернуться одна агрегированная строка
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": [
                        {
                            "Field": "CampaignId",
                            "Operator": "IN",
                            "Values": campaign_ids
                        }
                    ],
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "BounceRate",
                    "Cost",
                    "AvgCpc"
                ],
                "ReportName": f"Campaign Performance Summary Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "CUSTOM_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body,
                    timeout=60
                )
                
                # Устанавливаем кодировку UTF-8 для корректного отображения русских символов
                response.encoding = 'utf-8'
                
                if response.status_code == 400:
                    print("❌ Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 200:
                    print("✅ Сводный отчет создан успешно")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    
                    # Возвращаем данные отчета
                    result = {
                        'report': response.text,  # Содержимое отчета в TSV формате
                        'status': 'completed',
                        '_meta': {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'api_method': 'reports.post',
                            'api_version': 'v5',
                            'request_id': response.headers.get('RequestId', 'N/A'),
                            'format': 'TSV',
                            'type': 'summary'
                        }
                    }
                    return result
                    
                elif response.status_code == 201:
                    print("⏳ Сводный отчет поставлен в очередь в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 202:
                    print("⏳ Сводный отчет формируется в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 500:
                    print("❌ При формировании сводного отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 502:
                    print("❌ Время формирования сводного отчета превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                else:
                    print("❌ Произошла непредвиденная ошибка")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
            except requests.exceptions.ConnectionError:
                print("❌ Произошла ошибка соединения с сервером API")
                return None
                
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def create_ad_performance_report(self, campaign_ids: List[int], start_date: str, end_date: str, deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Создает отчет по производительности объявлений согласно официальному примеру"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Создание отчета по объявлениям: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем фильтры
        filters = [
            {
                "Field": "CampaignId",
                "Operator": "IN",
                "Values": campaign_ids
            }
        ]
        
        # Добавляем фильтр для исключения удаленных групп, если они есть
        if deleted_group_ids:
            filters.append({
                "Field": "AdGroupId",
                "Operator": "NOT_IN",
                "Values": deleted_group_ids
            })
            print(f"🚫 Исключаем {len(deleted_group_ids)} групп из отчета по объявлениям")
        
        # Формируем параметры отчета согласно официальному примеру
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": filters,
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "CampaignId",
                    "AdId",
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "BounceRate",
                    "Cost",
                    "AvgCpc"
                ],
                "ReportName": f"Ad Performance Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "AD_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body,
                    timeout=60
                )
                
                # Устанавливаем кодировку UTF-8 для корректного отображения русских символов
                response.encoding = 'utf-8'
                
                if response.status_code == 400:
                    print("❌ Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 200:
                    print("✅ Отчет по объявлениям создан успешно")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    
                    # Возвращаем данные отчета
                    result = {
                        'report': response.text,  # Содержимое отчета в TSV формате
                        'status': 'completed',
                        '_meta': {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'api_method': 'reports.post',
                            'api_version': 'v5',
                            'request_id': response.headers.get('RequestId', 'N/A'),
                            'format': 'TSV',
                            'type': 'ad_performance'
                        }
                    }
                    return result
                    
                elif response.status_code == 201:
                    print("⏳ Отчет по объявлениям поставлен в очередь в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 202:
                    print("⏳ Отчет по объявлениям формируется в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 500:
                    print("❌ При формировании отчета по объявлениям произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 502:
                    print("❌ Время формирования отчета по объявлениям превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                else:
                    print("❌ Произошла непредвиденная ошибка")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
            except requests.exceptions.ConnectionError:
                print("❌ Произошла ошибка соединения с сервером API")
                return None
                
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def create_adgroup_performance_report(self, campaign_ids: List[int], start_date: str, end_date: str, deleted_group_ids: List[int] = None) -> Optional[Dict]:
        """Создает отчет по производительности групп объявлений согласно официальному примеру"""
        if not campaign_ids:
            print("⚠️ Список ID кампаний пуст")
            return None
        
        print(f"🔍 Создание отчета по группам объявлений: {campaign_ids}")
        print(f"📅 Период: {start_date} - {end_date}")
        
        # Формируем фильтры
        filters = [
            {
                "Field": "CampaignId",
                "Operator": "IN",
                "Values": campaign_ids
            }
        ]
        
        # Добавляем фильтр для исключения удаленных групп, если они есть
        if deleted_group_ids:
            filters.append({
                "Field": "AdGroupId",
                "Operator": "NOT_IN",
                "Values": deleted_group_ids
            })
            print(f"🚫 Исключаем {len(deleted_group_ids)} групп из отчета по группам объявлений")
        
        # Формируем параметры отчета согласно официальному примеру
        body = {
            "params": {
                "SelectionCriteria": {
                    "Filter": filters,
                    "DateFrom": start_date,
                    "DateTo": end_date
                },
                "FieldNames": [
                    "CampaignId",
                    "AdGroupId",
                    "AdGroupName",
                    "CampaignType",
                    "AdNetworkType",
                    "Impressions",
                    "Clicks",
                    "Ctr",
                    "BounceRate",
                    "Cost",
                    "AvgCpc"
                ],
                "ReportName": f"AdGroup Performance Report {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "ReportType": "ADGROUP_PERFORMANCE_REPORT",
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "YES",
                "IncludeDiscount": "NO"
            }
        }
        
        # Добавляем заголовки согласно официальному примеру
        headers_with_processing = self.headers.copy()
        headers_with_processing["processingMode"] = "auto"
        
        # Цикл для выполнения запросов согласно официальному примеру
        while True:
            try:
                response = requests.post(
                    f"{self.base_url}/reports",
                    headers=headers_with_processing,
                    json=body,
                    timeout=60
                )
                
                # Устанавливаем кодировку UTF-8 для корректного отображения русских символов
                response.encoding = 'utf-8'
                
                if response.status_code == 400:
                    print("❌ Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 200:
                    print("✅ Отчет по группам объявлений создан успешно")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    
                    # Возвращаем данные отчета
                    result = {
                        'report': response.text,  # Содержимое отчета в TSV формате
                        'status': 'completed',
                        '_meta': {
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'api_method': 'reports.post',
                            'api_version': 'v5',
                            'request_id': response.headers.get('RequestId', 'N/A'),
                            'format': 'TSV',
                            'type': 'adgroup_performance'
                        }
                    }
                    return result
                    
                elif response.status_code == 201:
                    print("⏳ Отчет по группам объявлений поставлен в очередь в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 202:
                    print("⏳ Отчет по группам объявлений формируется в режиме офлайн")
                    retry_in = int(response.headers.get("retryIn", 60))
                    print(f"🔄 Повторная отправка запроса через {retry_in} секунд")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    time.sleep(retry_in)
                    
                elif response.status_code == 500:
                    print("❌ При формировании отчета по группам объявлений произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                elif response.status_code == 502:
                    print("❌ Время формирования отчета по группам объявлений превысило серверное ограничение.")
                    print("Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
                else:
                    print("❌ Произошла непредвиденная ошибка")
                    print(f"RequestId: {response.headers.get('RequestId', 'N/A')}")
                    print(f"JSON-код ответа сервера: {response.json()}")
                    return None
                    
            except requests.exceptions.ConnectionError:
                print("❌ Произошла ошибка соединения с сервером API")
                return None
                
            except Exception as e:
                print(f"❌ Произошла непредвиденная ошибка: {e}")
                return None
    
    def get_keywords_by_adgroups(self, adgroup_ids: List[int]) -> Optional[Dict]:
        """Получает ключевые фразы по ID групп объявлений"""
        if not adgroup_ids:
            print("⚠️ Список ID групп объявлений пуст")
            return None
        
        print(f"🔍 Получение ключевых фраз для групп: {len(adgroup_ids)} групп")
        
        # Разбиваем список групп на батчи по 10 групп
        batch_size = 10
        adgroup_batches = [adgroup_ids[i:i + batch_size] for i in range(0, len(adgroup_ids), batch_size)]
        
        print(f"📦 Разбиваем запрос на {len(adgroup_batches)} частей")
        
        method = 'keywords'
        field_names = [
            "Id",
            "Keyword",
            "AdGroupId",
            "CampaignId",
            "Status",
            "State",
            "Bid",
            "ContextBid",
            "StrategyPriority",
            "UserParam1",
            "UserParam2"
        ]
        
        all_keywords = []
        
        for batch_index, batch in enumerate(adgroup_batches, 1):
            print(f"\n🔄 Обработка части {batch_index}/{len(adgroup_batches)}")
            print(f"📊 Группы в текущей части: {batch}")
            
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {
                        "AdGroupIds": batch
                    },
                    "FieldNames": field_names
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/{method}",
                    headers=self.headers,
                    json=params,
                    timeout=60
                )
                
                response.encoding = 'utf-8'
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'result' in result and 'Keywords' in result['result']:
                        keywords = result['result']['Keywords']
                        all_keywords.extend(keywords)
                        print(f"✅ Получено ключевых фраз в текущей части: {len(keywords)}")
                    else:
                        print(f"⚠️ Нет ключевых фраз в части {batch_index}")
                        
                else:
                    print(f"❌ Ошибка API для части {batch_index}: {response.status_code}")
                    print(f"Response: {response.text}")
                    
            except Exception as e:
                print(f"❌ Ошибка обработки части {batch_index}: {e}")
                continue
        
        print(f"✅ Итого получено ключевых фраз: {len(all_keywords)}")
        
        # Формируем результат
        result = {
            "result": {
                "Keywords": all_keywords
            },
            "_meta": {
                "total_keywords": len(all_keywords),
                "total_adgroups": len(adgroup_ids),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        return result
    
    def get_image_urls_by_hashes(self, image_hashes: List[str]) -> Optional[Dict]:
        """Получает URL изображений по их хешам"""
        if not image_hashes:
            print("⚠️ Список хешей изображений пуст")
            return None
        
        print(f"🔍 Получение URL для {len(image_hashes)} изображений")
        
        # Разбиваем список хешей на батчи по 1000 хешей
        batch_size = 1000
        hash_batches = [image_hashes[i:i + batch_size] for i in range(0, len(image_hashes), batch_size)]
        
        print(f"📦 Разбиваем запрос на {len(hash_batches)} частей")
        
        method = 'adimages'
        field_names = [
            "AdImageHash",
            "Name",
            "Type",
            "Associated",
            "OriginalUrl",
            "PreviewUrl"
        ]
        
        all_images = []
        
        for batch_index, batch in enumerate(hash_batches, 1):
            print(f"\n🔄 Обработка части {batch_index}/{len(hash_batches)}")
            print(f"📊 Хешей в текущей части: {len(batch)}")
            
            params = {
                "method": "get",
                "params": {
                    "SelectionCriteria": {
                        "AdImageHashes": batch
                    },
                    "FieldNames": field_names
                }
            }
            
            try:
                response = requests.post(
                    f"{self.base_url}/{method}",
                    headers=self.headers,
                    json=params,
                    timeout=60
                )
                
                response.encoding = 'utf-8'
                
                if response.status_code == 200:
                    result = response.json()
                    
                    if 'result' in result and 'AdImages' in result['result']:
                        images = result['result']['AdImages']
                        all_images.extend(images)
                        print(f"✅ Получено изображений в текущей части: {len(images)}")
                    else:
                        print(f"⚠️ Нет изображений в части {batch_index}")
                        print(f"   Структура ответа: {list(result.keys())}")
                        if 'result' in result:
                            print(f"   Структура result: {list(result['result'].keys())}")
                        
                else:
                    print(f"❌ Ошибка API для части {batch_index}: {response.status_code}")
                    print(f"Response: {response.text}")
                    
            except Exception as e:
                print(f"❌ Ошибка обработки части {batch_index}: {e}")
                continue
        
        print(f"✅ Итого получено изображений: {len(all_images)}")
        
        # Формируем результат
        result = {
            "result": {
                "AdImages": all_images
            },
            "_meta": {
                "total_images": len(all_images),
                "total_hashes": len(image_hashes),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
        
        return result
    
    def get_sitelinks_by_set_id(self, sitelink_set_id: int) -> Optional[Dict]:
        """Получает быстрые ссылки по ID набора"""
        if not sitelink_set_id:
            print("⚠️ ID набора быстрых ссылок не указан")
            return None
        
        print(f"🔍 Получение быстрых ссылок для набора: {sitelink_set_id}")
        
        method = 'sitelinks'
        field_names = [
            "Id",
            "Sitelinks"
        ]
        
        params = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Ids": [sitelink_set_id]
                },
                "FieldNames": field_names
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{method}",
                headers=self.headers,
                json=params,
                timeout=60
            )
            
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                result = response.json()
                
                if 'result' in result and 'SitelinksSets' in result['result']:
                    sitelinks_sets = result['result']['SitelinksSets']
                    if sitelinks_sets and len(sitelinks_sets) > 0:
                        # Берем первый набор быстрых ссылок
                        sitelinks = sitelinks_sets[0].get('Sitelinks', [])
                        print(f"✅ Получено быстрых ссылок: {len(sitelinks)}")
                        
                        return {
                            "result": {
                                "SitelinksSets": sitelinks_sets
                            },
                            "_meta": {
                                "sitelink_set_id": sitelink_set_id,
                                "total_sitelinks": len(sitelinks),
                                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                        }
                    else:
                        print("⚠️ Пустой набор быстрых ссылок")
                        return None
                else:
                    print("⚠️ Нет быстрых ссылок в ответе")
                    print(f"   Структура ответа: {list(result.keys())}")
                    if 'result' in result:
                        print(f"   Структура result: {list(result['result'].keys())}")
                    return None
                    
            else:
                print(f"❌ Ошибка API: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения быстрых ссылок: {e}")
            return None
    
    def get_extensions_by_ids(self, extension_ids: List[int]) -> Optional[Dict]:
        """Получает расширения по списку ID"""
        if not extension_ids:
            print("⚠️ Список ID расширений пуст")
            return None
        
        print(f"🔍 Получение расширений для {len(extension_ids)} ID")
        
        method = 'adextensions'
        field_names = [
            "Id",
            "Type",
            "State",
            "Status",
            "StatusClarification",
            "Associated"
        ]
        
        params = {
            "method": "get",
            "params": {
                "SelectionCriteria": {
                    "Ids": extension_ids
                },
                "FieldNames": field_names,
                "CalloutFieldNames": [
                    "CalloutText"
                ]
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/{method}",
                headers=self.headers,
                json=params,
                timeout=60
            )
            
            response.encoding = 'utf-8'
            
            if response.status_code == 200:
                result = response.json()
                
                if 'result' in result and 'AdExtensions' in result['result']:
                    extensions = result['result']['AdExtensions']
                    print(f"✅ Получено расширений: {len(extensions)}")
                    
                    return {
                        "result": {
                            "AdExtensions": extensions
                        },
                        "_meta": {
                            "total_extensions": len(extensions),
                            "requested_ids": len(extension_ids),
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                    }
                else:
                    print("⚠️ Нет расширений в ответе")
                    print(f"   Структура ответа: {list(result.keys())}")
                    if 'result' in result:
                        print(f"   Структура result: {list(result['result'].keys())}")
                    return None
                    
            else:
                print(f"❌ Ошибка API: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Ошибка получения расширений: {e}")
            return None