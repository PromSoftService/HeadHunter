#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Тестовый скрипт для проверки телефонов через DaData.ru
"""

import sys
import requests
import re

# 🔑 ВСТАВЬТЕ СВОИ КЛЮЧИ СЮДА
API_KEY = "1f89f6298e5e8fc98500954a2a73498a58d39a24"  # Замените на свой
SECRET_KEY = "4cf476bf5820e46c89424e9fe534cc5f440f2b72"  # Замените на свой

def check_phone_dadata(phone: str):
    """Проверка через DaData"""
    url = "https://cleaner.dadata.ru/api/v1/clean/phone"
    headers = {
        "Authorization": f"Token {API_KEY}",
        "X-Secret": SECRET_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, headers=headers, json=[phone], timeout=10)
        
        if response.status_code == 200:
            data = response.json()[0]
            
            print(f"\n📞 Проверка: {phone}")
            print("-" * 50)
            print(f"✅ Статус API: {response.status_code}")
            print(f"\n📊 Результат DaData:")
            print(f"  • Телефон: {data.get('phone', '—')}")
            print(f"  • Страна: {data.get('country', '—')}")
            print(f"  • Город: {data.get('city', '—')}")
            print(f"  • Оператор: {data.get('provider', '—')}")
            print(f"  • Регион: {data.get('region', '—')}")
            print(f"  • QC: {data.get('qc', '—')}")
            
            # ⚠️ ВАЖНО: DaData сам определяет валидность
            # qc = 0 - идеально
            # qc = 1-2 - возможно устаревший, но существующий
            # qc = 3-5 - мусор или невалидный
            # Если phone = None - номер точно невалидный
            
            is_valid = False
            reasons = []
            
            # Проверка 1: есть ли телефон
            if not data.get('phone'):
                reasons.append("❌ phone = None (номер не распознан)")
            else:
                # Проверка 2: qc должен быть 0-2
                qc = data.get('qc', 5)
                if qc <= 2:
                    # Проверка 3: должен быть оператор
                    if data.get('provider'):
                        is_valid = True
                    else:
                        reasons.append(f"❌ нет оператора при qc={qc}")
                else:
                    reasons.append(f"❌ qc={qc} > 2 (мусор)")
            
            # Итоговый вердикт
            print("\n" + "=" * 50)
            if is_valid:
                print("✅✅✅ Номер ПРИЗНАН ВАЛИДНЫМ")
                print(f"    Оператор: {data.get('provider')}")
                print(f"    Регион: {data.get('region', '—')}")
            else:
                print("❌❌❌ Номер ОТВЕРГНУТ")
                for reason in reasons:
                    print(f"    {reason}")
            print("=" * 50)
                
        else:
            print(f"❌ Ошибка API: {response.status_code}")
            print(response.text)
            
    except requests.exceptions.Timeout:
        print("❌ Таймаут: сервер не отвечает")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

def main():
    if len(sys.argv) < 2:
        print("❌ Укажите номер телефона")
        print("\nПримеры:")
        print('  py test.py "+79611751003"')
        print('  py test.py "+70001751003"')
        print('  py test.py "8 (961) 175-10-03"')
        return
    
    phone = sys.argv[1]
    check_phone_dadata(phone)

if __name__ == "__main__":
    main()