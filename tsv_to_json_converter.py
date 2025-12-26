#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для преобразования TSV файлов статистики кампаний в JSON формат
"""

import json
import sys
import os
from typing import List, Dict, Any

def parse_tsv_to_json(tsv_content: str) -> Dict[str, Any]:
    """
    Преобразует TSV содержимое в JSON структуру
    """
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

def convert_tsv_file_to_json(tsv_file_path: str, json_file_path: str = None) -> bool:
    """
    Конвертирует TSV файл в JSON файл
    """
    try:
        # Читаем TSV файл
        with open(tsv_file_path, 'r', encoding='utf-8') as f:
            tsv_content = f.read()
        
        # Преобразуем в JSON
        json_data = parse_tsv_to_json(tsv_content)
        
        # Определяем путь для JSON файла
        if json_file_path is None:
            json_file_path = tsv_file_path.replace('.tsv', '.json')
        
        # Сохраняем JSON файл
        with open(json_file_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ Файл {tsv_file_path} успешно преобразован в {json_file_path}")
        print(f"📊 Обработано строк: {json_data['_meta']['total_rows']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка преобразования файла {tsv_file_path}: {e}")
        return False

def main():
    """Основная функция"""
    if len(sys.argv) < 2:
        print("Использование: python tsv_to_json_converter.py <путь_к_tsv_файлу> [путь_к_json_файлу]")
        print("Пример: python tsv_to_json_converter.py campaign_stats_1.tsv campaign_stats_1.json")
        return
    
    tsv_file = sys.argv[1]
    json_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if not os.path.exists(tsv_file):
        print(f"❌ Файл {tsv_file} не найден")
        return
    
    success = convert_tsv_file_to_json(tsv_file, json_file)
    if success:
        print("🎉 Преобразование завершено успешно!")
    else:
        print("💥 Преобразование завершилось с ошибкой")

if __name__ == "__main__":
    main()
