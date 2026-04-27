# -*- coding: utf-8 -*-
"""
Converted from IPYNB to PY
"""

# %% [code] Cell 1
# 📦 ЯЧЕЙКА 0: очистка и фильтрация

import pandas as pd
import os
from thefuzz import fuzz
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

base_dir = r"C:\Users\...\начальные материалы"
out_dir = r"C:\Users\...\кварт несост зак"
file_path = os.path.join(base_dir, "общая выгрузка_160426.xlsx")
out_file = os.path.join(out_dir, "оф_итог.xlsx")

print("📥 Загрузка данных...")
df = pd.read_excel(file_path, dtype=str)
df.columns = df.columns.str.strip().str.replace('✅', '', regex=False).str.strip()
df['_original_index'] = df.index.astype(str)

# Сохраняем номера как строки
number_columns = ['Реестровый номер лота', 'Реестровый номер извещения', 'Реестровый номер ГК', 
                  'Номер совместного лота', 'Номер экспертизы', 'ИНН победителя', 'Победители ИНН',
                  'ID лота']
for col in number_columns:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace('nan', '').replace('None', '')

print(f"   Загружено строк: {len(df):,}")

# ПАРСИНГ ДАТ
print("🔄 Преобразование дат...")

def parse_date_column(col):
    result = pd.to_datetime(col, format='%d.%m.%Y %H:%M:%S', errors='coerce')
    mask_na = result.isna()
    if mask_na.any():
        result2 = pd.to_datetime(col[mask_na], format='%d.%m.%Y', errors='coerce')
        result[mask_na] = result2
    return pd.to_datetime(result, errors='coerce')

df["Дата итогового протокола_dt"] = parse_date_column(df["Дата итогового протокола"])
df["Дата первой публикации_dt"] = parse_date_column(df["Дата первой публикации"])
df["Дата_для_фильтрации"] = df["Дата итогового протокола_dt"].fillna(df["Дата первой публикации_dt"])
df["Дата_для_фильтрации"] = pd.to_datetime(df["Дата_для_фильтрации"], errors='coerce')

print(f"   Дата протокола - распарсено: {df['Дата итогового протокола_dt'].notna().sum():,}")

# ФУНКЦИИ ДЕДУПЛИКАЦИИ

def joint_lot_dedup_with_flags(df):
    """
    1.2.4 Пустой номер совместного лота
    Все поля одинаковы, но в одном случае номер пустой, в другом - заполнен
    Помечаем как дубль строку с пустым номером, оставляем с заполненным
    """
    exclude = ['_original_index', 'Флаг дубля', 'Дубль сущности', 
               'Реестровый номер (сущность)', 'КПГЗ конечный (код) (сущность)',
               'Дата итогового протокола_dt', 'Дата первой публикации_dt', 'Дата_для_фильтрации']
    
    group_cols = [c for c in df.columns if c not in ["Номер совместного лота"] + exclude]
    
    df = df.copy()
    if 'Флаг дубля' not in df.columns:
        df['Флаг дубля'] = 0
        df['Дубль сущности'] = ''
    
    dups = df[df.duplicated(subset=group_cols, keep=False)]
    
    for _, group in dups.groupby(group_cols, dropna=False):
        if len(group) <= 1: continue
        joint_val = str(group["Совместный лот"].iloc[0]).strip().lower()
        if joint_val in ["нет", "", "nan"]:
            continue
        
        filled_mask = group["Номер совместного лота"].fillna('').astype(str).str.strip() != ""
        empty_mask = ~filled_mask
        
        if filled_mask.any() and empty_mask.any():
            df.loc[group.index[empty_mask], 'Флаг дубля'] = 1
            current_reason = df.loc[group.index[empty_mask], 'Дубль сущности'].fillna('')
            df.loc[group.index[empty_mask], 'Дубль сущности'] = current_reason + "; Пустой номер совместного лота"
    
    return df[df['Флаг дубля'] == 0].copy(), df[df['Флаг дубля'] == 1].copy()


def price_dedup_correct(df, price_col):
    """
    1.2.3 Разная начальная цена
    Все поля одинаковы, кроме цены => оставляем МАКСИМАЛЬНУЮ цену
    """
    df = df.copy()
    if 'Флаг дубля' not in df.columns:
        df['Флаг дубля'] = 0
        df['Дубль сущности'] = ''
    
    exclude = ['_original_index', 'Флаг дубля', 'Дубль сущности',
               'Дата итогового протокола_dt', 'Дата первой публикации_dt',
               'Дата_для_фильтрации', 'Реестровый номер (сущность)',
               'КПГЗ конечный (код) (сущность)', price_col]
    
    group_cols = [c for c in df.columns if c not in exclude]
    
    df[price_col + '_num'] = pd.to_numeric(df[price_col], errors='coerce')
    
    sort_cols = group_cols + [price_col + '_num']
    sort_asc = [True] * len(group_cols) + [False]
    df_sorted = df.sort_values(sort_cols, ascending=sort_asc, na_position='last')
    
    dups_mask = df_sorted.duplicated(subset=group_cols, keep='first')
    
    df_sorted.loc[dups_mask, 'Флаг дубля'] = 1
    current_reason = df_sorted.loc[dups_mask, 'Дубль сущности'].fillna('')
    df_sorted.loc[dups_mask, 'Дубль сущности'] = current_reason + f"; Отличается: {price_col} (оставлена макс. цена)"
    
    df_sorted = df_sorted.drop(columns=[price_col + '_num'], errors='ignore')
    df_sorted = df_sorted.sort_index()
    
    return df_sorted[df_sorted['Флаг дубля'] == 0].copy(), df_sorted[df_sorted['Флаг дубля'] == 1].copy()


def date_dedup_correct(df):
    """
    1.2.2 Разная дата протокола
    Все поля одинаковы, кроме даты => оставляем самую РАННЮЮ дату
    """
    df = df.copy()
    if 'Флаг дубля' not in df.columns:
        df['Флаг дубля'] = 0
        df['Дубль сущности'] = ''
    
    exclude = ['_original_index', 'Флаг дубля', 'Дубль сущности',
               'Дата итогового протокола_dt', 'Дата первой публикации_dt',
               'Дата_для_фильтрации', 'Дата итогового протокола', 
               'Дата первой публикации', 'Реестровый номер (сущность)',
               'КПГЗ конечный (код) (сущность)']
    
    group_cols = [c for c in df.columns if c not in exclude]
    
    sort_cols = group_cols + ['Дата итогового протокола_dt']
    sort_asc = [True] * len(group_cols) + [True]
    df_sorted = df.sort_values(sort_cols, ascending=sort_asc, na_position='last')
    
    dups_mask = df_sorted.duplicated(subset=group_cols, keep='first')
    
    df_sorted.loc[dups_mask, 'Флаг дубля'] = 1
    current_reason = df_sorted.loc[dups_mask, 'Дубль сущности'].fillna('')
    df_sorted.loc[dups_mask, 'Дубль сущности'] = current_reason + "; Отличается: Дата итогового протокола (оставлена ранняя)"
    
    df_sorted = df_sorted.sort_index()
    
    return df_sorted[df_sorted['Флаг дубля'] == 0].copy(), df_sorted[df_sorted['Флаг дубля'] == 1].copy()


def fuzzy_dedup_with_flags(df, fuzzy_cols, threshold=95):
    """
    1.2.1 Нечеткие дубли
    По fuzzy-полям - нечеткое сравнение, по остальным - точное совпадение
    Оставляем первую строку
    """
    exclude = ['_original_index', 'Флаг дубля', 'Дубль сущности', 
               'Дата итогового протокола_dt', 'Дата первой публикации_dt', 
               'Дата_для_фильтрации', 'Реестровый номер (сущность)', 
               'КПГЗ конечный (код) (сущность)']
    
    exact_cols = [c for c in df.columns if c not in fuzzy_cols and c not in exclude]
    fuzzy_cols = [c for c in fuzzy_cols if c in df.columns]
    exact_cols = [c for c in exact_cols if c in df.columns]
    
    df = df.copy()
    if 'Флаг дубля' not in df.columns:
        df['Флаг дубля'] = 0
        df['Дубль сущности'] = ''
    
    for c in exact_cols + fuzzy_cols:
        df[c] = df[c].fillna('').astype(str)
        
    groups = df.groupby(exact_cols, dropna=False)
    
    for _, group in tqdm(groups, desc="   🔍 Fuzzy-сравнение", leave=False):
        if len(group) <= 1: continue
        idx_list = group.index.tolist()
        for i in range(len(idx_list)):
            master = df.loc[idx_list[i]]
            for j in range(i+1, len(idx_list)):
                idx = idx_list[j]
                if df.loc[idx, 'Флаг дубля'] == 1: continue
                row = df.loc[idx]
                match = all(fuzz.token_set_ratio(str(master[fc]), str(row[fc])) >= threshold for fc in fuzzy_cols)
                if match:
                    df.loc[idx, 'Флаг дубля'] = 1
                    df.loc[idx, 'Дубль сущности'] = '; '.join([f"Нечеткое совпадение: {fc}" for fc in fuzzy_cols])
    
    return df[df['Флаг дубля'] == 0].copy(), df[df['Флаг дубля'] == 1].copy()


# ШАГ 1: Дедупликация
print("\n🧹 Шаг 1: Дедупликация по методологии...")

df['Флаг дубля'] = 0
df['Дубль сущности'] = ''
df['Реестровый номер (сущность)'] = df['Реестровый номер лота']
df['КПГЗ конечный (код) (сущность)'] = df['КПГЗ конечный (код)']

# 1. Совместные лоты (пустой номер)
df_clean, df_dups_joint = joint_lot_dedup_with_flags(df)
print(f"   1. После совместных лотов: {len(df_clean):,} строк (-{len(df_dups_joint):,})")

# 2. Разная цена (оставляем макс. цену)
price_col = "Начальная цена еденицы" if "Начальная цена еденицы" in df_clean.columns else "Начальная цена экспертизы"
if price_col in df_clean.columns:
    df_clean, df_dups_price = price_dedup_correct(df_clean, price_col)
    print(f"   2. После дедупликации по цене (макс): {len(df_clean):,} строк (-{len(df_dups_price):,})")
else:
    df_dups_price = pd.DataFrame()
    print(f"   2. Цена не найдена, пропуск")

# 3. Разная дата (оставляем раннюю дату)
df_clean, df_dups_date = date_dedup_correct(df_clean)
print(f"   3. После дедупликации по дате (ранняя): {len(df_clean):,} строк (-{len(df_dups_date):,})")

# 4. Нечеткие дубли
fuzzy_cols = ["Заказчик", "Главный заказчик", "Организатор", "ГРБС", "Комплекс", "Победитель наименование"]
df_clean, df_dups_fuzzy = fuzzy_dedup_with_flags(df_clean, fuzzy_cols)
print(f"   4. После нечеткой дедупликации: {len(df_clean):,} строк (-{len(df_dups_fuzzy):,})")

# Объединяем все дубли
df_dups_list = []
for dups_df in [df_dups_joint, df_dups_price, df_dups_date, df_dups_fuzzy]:
    if len(dups_df) > 0:
        df_dups_list.append(dups_df)

df_dups_v1 = pd.concat(df_dups_list, ignore_index=True) if df_dups_list else pd.DataFrame()
df_v1 = df_clean.reset_index(drop=True)

print(f"\n✅ После дедупликации: {len(df_v1):,} строк | Удалено дублей: {len(df_dups_v1):,}")

# ПРОВЕРКА: ЕСТЬ ЛИ ПОЛНЫЕ ДУБЛИ (ВСЕ ПОЛЯ ОДИНАКОВЫ)
print("\n🔍 Проверка на полные дубли (все поля одинаковы):")
exclude_check = ['_original_index', 'Флаг дубля', 'Дубль сущности', 
                 'Дата итогового протокола_dt', 'Дата первой публикации_dt',
                 'Дата_для_фильтрации', 'Реестровый номер (сущность)',
                 'КПГЗ конечный (код) (сущность)']
check_cols = [c for c in df_v1.columns if c not in exclude_check]
full_dups = df_v1[df_v1.duplicated(subset=check_cols, keep=False)]
if len(full_dups) > 0:
    print(f"   ⚠️ Осталось {len(full_dups)} строк-полных дублей!")
    print(f"   Уникальных групп с дублями: {full_dups.groupby(check_cols).ngroups}")
else:
    print(f"   ✅ Полных дублей нет!")

# ПРОВЕРКА КОНКРЕТНЫХ РЕЕСТРОВЫХ НОМЕРОВ
print("\n🔍 Проверка конкретных реестровых номеров:")
test_numbers = ['17022710', '16949236', '16914228', '16598624', '17053530']

for test_num in test_numbers:
    if 'Реестровый номер лота' in df_v1.columns:
        matches = df_v1[df_v1['Реестровый номер лота'].astype(str).str.strip() == test_num]
        status = "✅ 1 строка" if len(matches) == 1 else f"⚠️ {len(matches)} строк"
        print(f"   • {test_num}: {status}")

# ШАГ 2: Фильтрация данных
print("\n🔪 Шаг 2: Фильтрация данных...")
df_filtered = df_v1.copy()

initial_count = len(df_filtered)
print(f"   Исходно строк: {initial_count}")

# Статус
if "Статус процедуры" in df_filtered.columns:
    mask = df_filtered["Статус процедуры"].astype(str).str.strip().str.lower() == "торги завершены"
    df_filtered = df_filtered[mask]
    print(f"   После статуса: {len(df_filtered)} строк (-{initial_count - len(df_filtered):,})")

# Дата
start_date = pd.Timestamp("2026-01-01")
end_date = pd.Timestamp("2026-03-31 23:59:59")
if "Дата_для_фильтрации" in df_filtered.columns:
    before = len(df_filtered)
    df_filtered["Дата_для_фильтрации"] = pd.to_datetime(df_filtered["Дата_для_фильтрации"], errors='coerce')
    mask = (df_filtered["Дата_для_фильтрации"] >= start_date) & (df_filtered["Дата_для_фильтрации"] <= end_date)
    df_filtered = df_filtered[mask]
    print(f"   После даты (Q1 2026): {len(df_filtered)} строк (-{before - len(df_filtered):,})")

# Фонд реновации
exclude_org = "Московский фонд реновации жилой застройки"
if "Организатор" in df_filtered.columns:
    before = len(df_filtered)
    mask = ~(df_filtered["Организатор"].astype(str).str.contains(exclude_org, na=False, case=False) | 
             df_filtered["Заказчик"].astype(str).str.contains(exclude_org, na=False, case=False))
    df_filtered = df_filtered[mask]
    print(f"   После исключения фонда: {len(df_filtered)} строк (-{before - len(df_filtered):,})")

# Единственный поставщик
exclude_method = "Закупка у единственного поставщика"
if "Способ определения поставщика" in df_filtered.columns:
    before = len(df_filtered)
    mask = ~df_filtered["Способ определения поставщика"].astype(str).str.contains(exclude_method, na=False, case=False)
    df_filtered = df_filtered[mask]
    print(f"   После исключения ед. поставщика: {len(df_filtered)} строк (-{before - len(df_filtered):,})")

# Удаляем временные колонки
cols_to_drop = ["Дата итогового протокола_dt", "Дата первой публикации_dt", "Дата_для_фильтрации", "_original_index"]
df_itog = df_filtered.drop(columns=[c for c in cols_to_drop if c in df_filtered.columns], errors='ignore').reset_index(drop=True)

print(f"\n✅ Итог: {len(df_itog):,} строк")

=== ПРОВЕРКА В ИТОГЕ
print("\n🔍 Проверка в итоге:")
for test_num in test_numbers:
    matches = df_itog[df_itog['Реестровый номер лота'].astype(str).str.strip() == test_num]
    status = "✅ 1 строка" if len(matches) == 1 else f"⚠️ {len(matches)} строк"
    print(f"   • {test_num}: {status}")

# ШАГ 3: Пустые КПГЗ и НМЦ
def get_empty_kpgz_mask(d):
    kpgz3_empty = d["КПГЗ-3 (код)"].isna() | (d["КПГЗ-3 (код)"].astype(str).str.strip() == "")
    kpgz_final_empty = d["КПГЗ конечный (код)"].isna() | (d["КПГЗ конечный (код)"].astype(str).str.strip() == "")
    mask_empty = kpgz3_empty | kpgz_final_empty
    
    is_joint = d["Совместный лот"].fillna('').astype(str).str.strip().str.lower() != "нет"
    try:
        num_joint = d["Номер совместного лота"].astype(str).str.strip()
        num_reg = d["Реестровый номер лота"].astype(str).str.strip()
        is_root = is_joint & (num_joint == num_reg)
    except:
        is_root = pd.Series(False, index=d.index)
    return mask_empty & ~is_root

empty_kpgz_itog = df_itog[get_empty_kpgz_mask(df_itog)] if len(df_itog) > 0 else pd.DataFrame()
empty_nmc_itog = df_itog[df_itog["НМЦ закупки"].isna() | (df_itog["НМЦ закупки"].astype(str).str.strip() == "")] if len(df_itog) > 0 else pd.DataFrame()

print(f"\n📊 Пустых КПГЗ: {len(empty_kpgz_itog):,}")
print(f"📊 Пустых НМЦ закупки: {len(empty_nmc_itog):,}")

# ШАГ 4: Уникальные закупки (только корневые и несовместные)
def get_unique_purchases(df):
    """
    Возвращает уникальные закупки по реестровому номеру извещения.
    Для совместных лотов берём ТОЛЬКО корневые лоты (где Реестровый номер лота = Номер совместного лота).
    Для несовместных лотов берём все одиночные.
    """
    if len(df) == 0:
        return pd.DataFrame()
    
    # Определяем совместные лоты
    is_joint = df["Совместный лот"].fillna('').astype(str).str.strip().str.lower() != "нет"
    
    # Для совместных лотов определяем корневые
    is_root = pd.Series(False, index=df.index)
    if is_joint.any():
        num_joint = df["Номер совместного лота"].fillna('').astype(str).str.strip()
        num_reg = df["Реестровый номер лота"].fillna('').astype(str).str.strip()
        is_root = is_joint & (num_joint == num_reg) & (num_joint != '')
    
    # Правильная строка = корневой лот ИЛИ несовместный лот
    is_correct = is_root | ~is_joint
    
    # Берём только правильные строки
    correct_rows = df[is_correct].copy()
    
    # Убираем дубли по реестровому номеру извещения
    if 'Реестровый номер извещения' in correct_rows.columns:
        correct_rows = correct_rows.drop_duplicates(subset=['Реестровый номер извещения'], keep='first')
    
    return correct_rows

unique_purchases_itog = get_unique_purchases(df_itog)
print(f"\n📊 Уникальных закупок в итоге: {len(unique_purchases_itog):,}")

# ШАГ 4.1: Расчёт среднего количества участников (только при ≥1 заявке)
if len(unique_purchases_itog) > 0 and "Подано заявок" in unique_purchases_itog.columns:
    # Преобразуем в число и фильтруем только те, где подано 1 и более заявок
    participants_num = pd.to_numeric(unique_purchases_itog["Подано заявок"], errors='coerce')
    valid_participants_mask = participants_num >= 1
    valid_participants = participants_num[valid_participants_mask]
    
    if len(valid_participants) > 0:
        avg_participants = valid_participants.mean()
        print(f"📊 Среднее кол-во участников (при ≥1 заявке): {avg_participants:.2f} (из {len(valid_participants):,} закупок)")
    else:
        print(f"📊 Нет закупок с ≥1 заявкой для расчёта среднего")
else:
    print(f"📊 Нет данных для расчёта среднего количества участников")

# ШАГ 5: Сохранение
print("\n💾 Сохранение в Excel...")

with pd.ExcelWriter(out_file, engine='openpyxl') as writer:
    df_itog.to_excel(writer, sheet_name="итог", index=False)
    
    if len(df_dups_date) > 0:
        df_dups_date.to_excel(writer, sheet_name="(итог) дубли по дате", index=False)
    if len(df_dups_price) > 0:
        df_dups_price.to_excel(writer, sheet_name="(итог) дубли по цене", index=False)
    if len(df_dups_joint) > 0:
        df_dups_joint.to_excel(writer, sheet_name="(итог) дубли совместных лотов", index=False)
    if len(df_dups_fuzzy) > 0:
        df_dups_fuzzy.to_excel(writer, sheet_name="(итог) нечеткие дубли", index=False)
    
    if len(empty_kpgz_itog) > 0:
        empty_kpgz_itog.to_excel(writer, sheet_name="(итог) пустые кпгз", index=False)
    if len(empty_nmc_itog) > 0:
        empty_nmc_itog.to_excel(writer, sheet_name="(итог) пустые нмц закупки", index=False)
    
    unique_purchases_itog.to_excel(writer, sheet_name="уникальные закупки итог", index=False)
    df_v1.to_excel(writer, sheet_name="исх", index=False)

print(f"""
✅ ГОТОВО! Файл: {out_file}

📊 Статистика:
   • Исходно: {len(df):,} строк
   • После дедупликации: {len(df_v1):,} строк (-{len(df):,} - {len(df_v1):,})
   • После фильтрации: {len(df_itog):,} строк
   • Уникальных закупок: {len(unique_purchases_itog):,}
   • Дублей по дате: {len(df_dups_date):,}
   • Дублей по цене: {len(df_dups_price):,}
   • Пустых совместных номеров: {len(df_dups_joint):,}
   • Нечетких дублей: {len(df_dups_fuzzy):,}
""")

# %% [code] Cell 2
# 📦 ЯЧЕЙКА 1: анализ эффективности закупок (оф_итог)
# БЛОК 1: Все закупки (лист "уникальные закупки итог")
# БЛОК 2: ПЦП закупки (лист "итог" с корректным расчетом НМЦ через корневые лоты)

import pandas as pd
import numpy as np
import os
from datetime import datetime
from IPython.display import display
import warnings
warnings.filterwarnings('ignore')

# Настройки отображения
pd.set_option('display.float_format', '{:,.2f}'.format)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

base_dir = r"C:\Users\leoga\Desktop\ГБУ МосЗакупки\кварт несост зак"
base_dir1 = r"C:\Users\leoga\Desktop\ГБУ МосЗакупки\кварт несост зак\рабочие файлы\анализ эфф закупок"
file_path = os.path.join(base_dir, "оф_итог_1404.xlsx")

# 🔵 БЛОК 1: АНАЛИТИКА ПО ВСЕМ ЗАКУПКАМ (на основе листа "уникальные закупки итог")

print("\n" + "="*70)
print("🔵 БЛОК 1: ЗАГРУЗКА ВСЕХ ЗАКУПОК")
print("="*70)

# 1. Загрузка листа "уникальные закупки итог"
print("📥 Загрузка листа 'уникальные закупки итог'...")
df_all = pd.read_excel(file_path, sheet_name="уникальные закупки итог")
df_all.columns = df_all.columns.str.strip().str.replace('✅', '', regex=False).str.strip()

UNIQ_COL = "Реестровый номер извещения"
NMC_COL  = "НМЦ закупки"
COMP_COL = "Допущено участников"
LAW_COL  = "Закон-основание"
METH_COL = "Способ определения поставщика"

print(f"📂 Загружено строк: {len(df_all):,}")

# 2. Приведение числовых типов
money_columns = ['НМЦ закупки', 'НМЦ спецификации', 'НМЦ ГК спецификации',
                 'Начальная цена экспертизы', 'Рекомендованная цена', 'Цена ГК',
                 'Цена ГК по спецификации', 'Исполнение договора в руб']
for col in money_columns:
    if col in df_all.columns:
        df_all[col] = pd.to_numeric(df_all[col], errors='coerce').fillna(0)

if COMP_COL in df_all.columns:
    df_all[COMP_COL] = pd.to_numeric(df_all[COMP_COL], errors='coerce').fillna(0)

# 3. Удаление дублей
uniq_check = df_all[UNIQ_COL].nunique()
total_rows = len(df_all)

if total_rows != uniq_check:
    print(f"   ⚠️ Обнаружено {total_rows - uniq_check} дублей. Удаляем...")
    df_all = df_all.drop_duplicates(subset=[UNIQ_COL], keep='first').copy()

# 4. Итоговые показатели (Блок 1)
total_cnt_all = len(df_all)
total_nmc_all = df_all[NMC_COL].sum()

print(f"\n📈 ГЛОБАЛЬНЫЕ ПОКАЗАТЕЛИ (ВСЕ ЗАКУПКИ):")
print(f"   • Уникальных закупок: {total_cnt_all:,}")
print(f"   • Общая НМЦ: {total_nmc_all:,.2f} руб.\n")

# 5. Категория конкуренции
def comp_cat(n):
    if n >= 2: return "2+_уч"
    elif n == 1: return "1_уч"
    return "0_уч"

df_all["кат_конк"] = df_all[COMP_COL].apply(comp_cat)

# 📊 ТАБЛИЦА 1 (ВСЕ): Конкуренция
t1_all = df_all.groupby("кат_конк", observed=False).agg(
    НМЦ=(NMC_COL, "sum"),
    кол_во=(UNIQ_COL, "count")
).rename(columns={"кол_во": "кол-во"}).reindex(["2+_уч", "1_уч", "0_уч"], fill_value=0)

t1_all["НМЦ %"] = (t1_all["НМЦ"] / total_nmc_all * 100).round(1).astype(str) + "%" if total_nmc_all > 0 else "0.0%"
t1_all["кол-во %"] = (t1_all["кол-во"] / total_cnt_all * 100).round(1).astype(str) + "%" if total_cnt_all > 0 else "0.0%"
t1_all["НМЦ"] = t1_all["НМЦ"].round(2)

print("📋 ТАБЛИЦА 1 (ВСЕ): Распределение по конкурентности")
display(t1_all[["НМЦ", "НМЦ %", "кол-во", "кол-во %"]])
print("\n" + "="*70 + "\n")

# 📊 ТАБЛИЦА 2 (ВСЕ): Федеральные законы
df_all["_law_num"] = df_all[LAW_COL].astype(str).str.extract(r'(\d+)')[0]
m44_all  = df_all["_law_num"] == "44"
m223_all = df_all["_law_num"] == "223"

t2_all_data = []
for name, mask in [("44фз", m44_all), ("223фз", m223_all)]:
    t2_all_data.append({
        "ФЗ": name,
        "НМЦ (руб)": round(df_all.loc[mask, NMC_COL].sum(), 2),
        "кол-во закупок": int(mask.sum())
    })
t2_all = pd.DataFrame(t2_all_data).set_index("ФЗ")
print("📋 ТАБЛИЦА 2 (ВСЕ): Разрез по Федеральным законам")
display(t2_all)
print("\n" + "="*70 + "\n")

# 📊 ТАБЛИЦА 3 (ВСЕ): Способы закупки
bids_col = "Подано заявок"
method_map = {
    "Электронный конкурс": "конкурсы", 
    "Электронный аукцион": "аукционы",
    "Электронный запрос котировок": "котировки", 
    "Электронный запрос предложений": "запросы"
}
df_all["_method"] = df_all[METH_COL].map(method_map)

t3_all_data = []
for k in ["конкурсы", "аукционы", "котировки", "запросы"]:
    g = df_all[df_all["_method"] == k]
    if bids_col in df_all.columns:
        g_filtered = g[g[bids_col] >= 1]
    else:
        g_filtered = g
    avg_comp = round(g_filtered[COMP_COL].mean(), 2) if len(g_filtered) > 0 else 0
    t3_all_data.append({
        "Способ": k.capitalize(),
        "Уникальных закупок": len(g),
        "Сумма НМЦ (руб)": round(g[NMC_COL].sum(), 2),
        "Ср. допущено участников": avg_comp
    })

kz_all = df_all[df_all["_method"].isin(["котировки", "запросы"])]
if bids_col in df_all.columns:
    kz_filtered_all = kz_all[kz_all[bids_col] >= 1]
else:
    kz_filtered_all = kz_all

t3_all_data.append({
    "Способ": "Котировки+Запросы",
    "Уникальных закупок": len(kz_all),
    "Сумма НМЦ (руб)": round(kz_all[NMC_COL].sum(), 2),
    "Ср. допущено участников": round(kz_filtered_all[COMP_COL].mean(), 2) if len(kz_filtered_all) > 0 else 0
})

t3_all = pd.DataFrame(t3_all_data).set_index("Способ")
print("📋 ТАБЛИЦА 3 (ВСЕ): Разрез по способам определения поставщика")
display(t3_all)


# 🟢 БЛОК 2: АНАЛИТИКА ПО ПЦП-ЗАКУПКАМ (на основе листа "итог" с логикой корневых лотов)

print("\n" + "="*70)
print("🟢 БЛОК 2: АНАЛИТИКА ПО ПЦП (лист 'итог', расчет НМЦ через корневые лоты)")
print("="*70 + "\n")

# 1. Загрузка листа "итог"
print("📥 Загрузка листа 'итог' для детального анализа ПЦП...")
df_pzp_raw = pd.read_excel(file_path, sheet_name="итог", dtype={'Реестровый номер извещения': str, 'Реестровый номер лота': str})
df_pzp_raw.columns = df_pzp_raw.columns.str.strip().str.replace('✅', '', regex=False).str.strip()

LOT_COL = "Реестровый номер лота"
JOINT_COL = "Номер совместного лота"
METH_NMC_COL = "Метод определения НМЦК"
KPGZ3_COL = "КПГЗ-3 (код)"

print(f"📂 Загружено строк (сырые): {len(df_pzp_raw):,}")

# 2. Чистка чисел (как в Коде 2)
def clean_russian_numbers(series):
    s = series.astype(str).str.strip()
    s = s.str.replace(' ', '', regex=False)
    s = s.str.replace(',', '.', regex=False)
    return pd.to_numeric(s, errors='coerce')

money_cols = ['НМЦ закупки', 'НМЦ спецификации']
for col in money_cols:
    if col in df_pzp_raw.columns:
        df_pzp_raw[col] = clean_russian_numbers(df_pzp_raw[col]).fillna(0)

if COMP_COL in df_pzp_raw.columns:
    df_pzp_raw[COMP_COL] = pd.to_numeric(df_pzp_raw[COMP_COL], errors='coerce').fillna(0)

# 3. ОПРЕДЕЛЕНИЕ ТИПА ЛОТА (Корневой / Вложенный / Несовместный)
# Вложенный: есть "Номер совместного лота" И он не равен "Реестровый номер лота"
# Корневой: есть "Номер совместного лота" И он равен "Реестровый номер лота"
# Несовместный: нет "Номер совместного лота"

# Приводим к числам для корректного сравнения
df_pzp_raw['_joint_num'] = pd.to_numeric(df_pzp_raw[JOINT_COL], errors='coerce')
df_pzp_raw['_lot_num'] = pd.to_numeric(df_pzp_raw[LOT_COL], errors='coerce')

# Условие вложенности: номер совместного есть И номер совместного != номер лота
mask_child = df_pzp_raw['_joint_num'].notna() & (df_pzp_raw['_joint_num'] != df_pzp_raw['_lot_num'])
df_pzp_raw['_is_child'] = mask_child

# 4. РАСЧЕТ НМЦ ЗАКУПКИ (Берется 1 раз на закупку из корневых или несовместных лотов)
print("⚙️ Расчет корректной НМЦ закупки (исключая вложенные лоты)...")

# Берем строки, которые НЕ являются вложенными (т.е. корневые или несовместные)
df_valid_nmc = df_pzp_raw[~df_pzp_raw['_is_child']].copy()

# Группируем по закупке и берем первую попавшуюся НМЦ (она должна быть одинаковой)
purchase_nmc_map = df_valid_nmc.groupby(UNIQ_COL)[NMC_COL].first()

# Мапим эту НМЦ на ВСЕ строки (включая вложенные), чтобы у лота была правильная НМЦ закупки
df_pzp_raw['НМЦ закупки'] = df_pzp_raw[UNIQ_COL].map(purchase_nmc_map).fillna(0)

# 5. Расчет флага ПЦП
if METH_NMC_COL in df_pzp_raw.columns and KPGZ3_COL in df_pzp_raw.columns:
    df_pzp_raw[METH_NMC_COL] = df_pzp_raw[METH_NMC_COL].astype(str).fillna('')
    df_pzp_raw[KPGZ3_COL] = df_pzp_raw[KPGZ3_COL].astype(str).fillna('')
    
    mask_norm = df_pzp_raw[METH_NMC_COL] == "Нормативный метод"
    mask_other = (df_pzp_raw[METH_NMC_COL].str.contains("Иной метод", case=False, na=False)) & \
                 (df_pzp_raw[KPGZ3_COL].str.startswith("03.10.01"))
    
    df_pzp_raw["is_pzp_raw"] = mask_norm | mask_other
    
    # Если хоть один лот в закупке ПЦП -> вся закупка ПЦП
    pzp_map = df_pzp_raw.groupby(UNIQ_COL)["is_pzp_raw"].max()
    df_pzp_raw["is_pzp"] = df_pzp_raw[UNIQ_COL].map(pzp_map).fillna(False)
else:
    df_pzp_raw["is_pzp"] = False

# 6. ФИЛЬТРАЦИЯ: оставляем только ПЦП
df_pzp = df_pzp_raw[df_pzp_raw["is_pzp"]].copy()
print(f"🏷️ Отфильтровано ПЦП-закупок: {df_pzp[UNIQ_COL].nunique():,}")

# 7. Убираем дубликаты лотов, оставляя 1 запись на закупку для анализа
df_pzp_uniq = df_pzp.drop_duplicates(subset=[UNIQ_COL], keep='first').copy()

# 8. Расчет показателей по ПЦП
pzp_cnt = len(df_pzp_uniq)
pzp_nmc = df_pzp_uniq[NMC_COL].sum()

print(f"\n📈 ПОКАЗАТЕЛИ ПО ПЦП:")
print(f"   • Уникальных закупок: {pzp_cnt:,}")
print(f"   • Общая НМЦ: {pzp_nmc:,.2f} руб.\n")
print("="*70)

# 9. Категория конкуренции (ПЦП)
df_pzp_uniq["кат_конк"] = df_pzp_uniq[COMP_COL].apply(comp_cat)

# 📊 ТАБЛИЦА 1 (ПЦП): Конкуренция
t1_pzp = df_pzp_uniq.groupby("кат_конк", observed=False).agg(
    НМЦ=(NMC_COL, "sum"),
    кол_во=(UNIQ_COL, "count")
).rename(columns={"кол_во": "кол-во"}).reindex(["2+_уч", "1_уч", "0_уч"], fill_value=0)

t1_pzp["НМЦ %"] = (t1_pzp["НМЦ"] / pzp_nmc * 100).round(1).astype(str) + "%" if pzp_nmc > 0 else "0.0%"
t1_pzp["кол-во %"] = (t1_pzp["кол-во"] / pzp_cnt * 100).round(1).astype(str) + "%" if pzp_cnt > 0 else "0.0%"
t1_pzp["НМЦ"] = t1_pzp["НМЦ"].round(2)

print("📋 ТАБЛИЦА 1 (ПЦП): Распределение по конкурентности")
display(t1_pzp[["НМЦ", "НМЦ %", "кол-во", "кол-во %"]])
print("\n" + "="*70 + "\n")

# 📊 ТАБЛИЦА 2 (ПЦП): Федеральные законы
df_pzp_uniq["_law_num"] = df_pzp_uniq[LAW_COL].astype(str).str.extract(r'(\d+)')[0]
m44_pzp  = df_pzp_uniq["_law_num"] == "44"
m223_pzp = df_pzp_uniq["_law_num"] == "223"

t2_pzp_data = []
for name, mask in [("44фз", m44_pzp), ("223фз", m223_pzp)]:
    t2_pzp_data.append({
        "ФЗ": name,
        "НМЦ (руб)": round(df_pzp_uniq.loc[mask, NMC_COL].sum(), 2),
        "кол-во закупок": int(mask.sum())
    })
t2_pzp = pd.DataFrame(t2_pzp_data).set_index("ФЗ")
print("📋 ТАБЛИЦА 2 (ПЦП): Разрез по Федеральным законам")
display(t2_pzp)
print("\n" + "="*70 + "\n")

# 📊 ТАБЛИЦА 3 (ПЦП): Способы закупки
df_pzp_uniq["_method"] = df_pzp_uniq[METH_COL].map(method_map)

t3_pzp_data = []
for k in ["конкурсы", "аукционы", "котировки", "запросы"]:
    g = df_pzp_uniq[df_pzp_uniq["_method"] == k]
    if bids_col in df_pzp_uniq.columns:
        g_filtered = g[g[bids_col] >= 1]
    else:
        g_filtered = g
    avg_comp = round(g_filtered[COMP_COL].mean(), 2) if len(g_filtered) > 0 else 0
    
    t3_pzp_data.append({
        "Способ": k.capitalize(),
        "Уникальных закупок": len(g),
        "Сумма НМЦ (руб)": round(g[NMC_COL].sum(), 2),
        "Ср. допущено участников": avg_comp
    })

kz_pzp = df_pzp_uniq[df_pzp_uniq["_method"].isin(["котировки", "запросы"])]
if bids_col in df_pzp_uniq.columns:
    kz_pzp_filtered = kz_pzp[kz_pzp[bids_col] >= 1]
else:
    kz_pzp_filtered = kz_pzp

t3_pzp_data.append({
    "Способ": "Котировки+Запросы",
    "Уникальных закупок": len(kz_pzp),
    "Сумма НМЦ (руб)": round(kz_pzp[NMC_COL].sum(), 2),
    "Ср. допущено участников": round(kz_pzp_filtered[COMP_COL].mean(), 2) if len(kz_pzp_filtered) > 0 else 0
})

t3_pzp = pd.DataFrame(t3_pzp_data).set_index("Способ")

if bids_col in df_pzp_uniq.columns:
    mask_bids_pzp = df_pzp_uniq[bids_col] >= 1
    overall_avg_pzp = round(df_pzp_uniq.loc[mask_bids_pzp, COMP_COL].mean(), 2) if mask_bids_pzp.any() else 0
    print(f"🌐 СРЕДНЕЕ ПЦП (Подано заявок ≥ 1): {overall_avg_pzp}\n")

print("📋 ТАБЛИЦА 3 (ПЦП): Разрез по способам определения поставщика")
display(t3_pzp)
print("\n" + "="*70 + "\n")


# 🔍 ИТОГОВАЯ СВЕРКА И ЭКСПОРТ

print("🔍 ПРОВЕРКА КОРРЕКТНОСТИ:")

# Сверка Блока 1
print(f"   [БЛОК 1 - ВСЕ] Т1 НМЦ: {t1_all['НМЦ'].sum():,.2f} / {total_nmc_all:,.2f} {'✅' if abs(t1_all['НМЦ'].sum()-total_nmc_all)<1 else '❌'}")
print(f"   [БЛОК 1 - ВСЕ] Т1 Закупки: {t1_all['кол-во'].sum()} / {total_cnt_all} {'✅' if t1_all['кол-во'].sum()==total_cnt_all else '❌'}")

# Сверка Блока 2
print(f"   [БЛОК 2 - ПЦП] Т1 НМЦ: {t1_pzp['НМЦ'].sum():,.2f} / {pzp_nmc:,.2f} {'✅' if abs(t1_pzp['НМЦ'].sum()-pzp_nmc)<1 else '❌'}")
print(f"   [БЛОК 2 - ПЦП] Т1 Закупки: {t1_pzp['кол-во'].sum()} / {pzp_cnt} {'✅' if t1_pzp['кол-во'].sum()==pzp_cnt else '❌'}")

# ЭКСПОРТ В EXCEL
ts = datetime.now().strftime("%Y-%m-%d_%H%M")
EXPORT_TO_EXCEL = False

if EXPORT_TO_EXCEL:
    # Подготовка DF для экспорта
    cols_to_drop = ['_law_num', '_method', 'кат_конк', '_joint_num', '_lot_num', '_is_child', 'is_pzp', 'is_pzp_raw']
    
    df_all_export = df_all.drop(columns=[c for c in cols_to_drop if c in df_all.columns], errors='ignore')
    df_pzp_export = df_pzp.drop(columns=[c for c in cols_to_drop if c in df_pzp.columns], errors='ignore')

    # Файл 1: Все закупки
    fp1 = os.path.join(base_dir1, f"анализ_все_{ts}.xlsx")
    with pd.ExcelWriter(fp1, engine='openpyxl') as writer:
        t1_all.to_excel(writer, sheet_name="свод_конк")
        t2_all.to_excel(writer, sheet_name="свод_фз")
        t3_all.to_excel(writer, sheet_name="свод_методы")
        df_all_export.to_excel(writer, sheet_name="детали", index=False)
    print(f"💾 Сохранен анализ ВСЕХ закупок: {fp1}")

    # Файл 2: ПЦП закупки
    fp2 = os.path.join(base_dir1, f"анализ_пцп_{ts}.xlsx")
    with pd.ExcelWriter(fp2, engine='openpyxl') as writer:
        t1_pzp.to_excel(writer, sheet_name="свод_конк")
        t2_pzp.to_excel(writer, sheet_name="свод_фз")
        t3_pzp.to_excel(writer, sheet_name="свод_методы")
        df_pzp_export.to_excel(writer, sheet_name="детали", index=False)
    print(f"💾 Сохранен анализ ПЦП закупок: {fp2}")
    
    print("✅ Все файлы успешно выгружены.")

# %% [code] Cell 3
# 📦 ПОЛНЫЙ АНАЛИЗ КПГЗ-3 (ФИНАЛЬНАЯ ВЕРСИЯ ПО МЕТОДОЛОГИИ)
import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ПУТИ
base_dir = r"C:\Users\leoga\Desktop\ГБУ МосЗакупки\кварт несост зак"
out_dir = r"C:\Users\leoga\Desktop\ГБУ МосЗакупки\кварт несост зак\рабочие файлы\кпгз"
os.makedirs(out_dir, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M")

input_file = os.path.join(base_dir, "оф_итог_1404.xlsx")
df = pd.read_excel(input_file, sheet_name="итог", dtype={'Реестровый номер извещения': str, 'Реестровый номер лота': str})
print(f"📥 Загружено строк: {len(df)}")


# ИСПРАВЛЕНИЕ ФОРМАТА ЧИСЕЛ
def clean_russian_numbers(series):
    s = series.astype(str).str.strip()
    s = s.str.replace(' ', '', regex=False)
    s = s.str.replace(',', '.', regex=False)
    return pd.to_numeric(s, errors='coerce')

money_cols = ['НМЦ закупки', 'НМЦ спецификации']
for col in money_cols:
    if col in df.columns:
        df[col] = clean_russian_numbers(df[col])


# 1. ПОДГОТОВКА ДАННЫХ
df['Реестровый номер извещения'] = df['Реестровый номер извещения'].astype(str).str.strip()
df['КПГЗ-3 (код)'] = df['КПГЗ-3 (код)'].fillna(df['КПГЗ конечный (код)'])
df['КПГЗ-3 (наименование)'] = df['КПГЗ-3 (наименование)'].fillna(df['КПГЗ конечный (наименование)'])
df['КПГЗ-3 (код)'] = df['КПГЗ-3 (код)'].replace('', np.nan)
df['КПГЗ-3 (наименование)'] = df['КПГЗ-3 (наименование)'].replace('', np.nan)

for col in ['Метод определения НМЦК', 'Победители ИНН']:
    if col not in df.columns:
        print(f"   ⚠️ Колонка '{col}' не найдена. Будет создана пустая.")
        df[col] = np.nan

empty_mask = df['КПГЗ-3 (код)'].isna()
if empty_mask.any():
    reestr_all_empty = df[empty_mask].groupby('Реестровый номер извещения').filter(lambda x: x['КПГЗ-3 (код)'].isna().all()).index.unique()
    df.loc[df['Реестровый номер извещения'].isin(reestr_all_empty), 'КПГЗ-3 (код)'] = 'НЕ_ОПРЕДЕЛЕНО'
    df.loc[df['Реестровый номер извещения'].isin(reestr_all_empty), 'КПГЗ-3 (наименование)'] = 'Не определено'
    print(f"   ⚠️ Заполнено НЕ_ОПРЕДЕЛЕНО для {len(reestr_all_empty)} закупок")

print(f"   Уникальных закупок: {df['Реестровый номер извещения'].nunique():,}")


# 2. ОПРЕДЕЛЕНИЕ КОРНЕВЫХ ЛОТОВ
def is_root_lot(row):
    n1 = pd.to_numeric(row["Номер совместного лота"], errors='coerce')
    n2 = pd.to_numeric(row["Реестровый номер лота"], errors='coerce')
    return not pd.isna(n1) and not pd.isna(n2) and n1 == n2

df['_is_root'] = df.apply(is_root_lot, axis=1)


# 3. МЕТАДАННЫЕ ПО ЗАКУПКЕ
def extract_purchase_meta(group):
    is_root = group.apply(is_root_lot, axis=1)
    root_row = group[is_root]
    
    if not root_row.empty:
        nmc_val = root_row['НМЦ закупки'].dropna()
        total_nmc = nmc_val.iloc[0] if len(nmc_val) > 0 else 0.0
    else:
        nmc_val = group['НМЦ закупки'].dropna()
        total_nmc = nmc_val.iloc[0] if len(nmc_val) > 0 else 0.0
        
    unit_flag = group['Признак закупки со стоимостью за единицу'].dropna()
    flag = unit_flag.iloc[0] if len(unit_flag) > 0 else 0
    
    custs = group['Заказчик'].dropna().astype(str).str.strip()
    all_custs = custs.str.split(',').explode().str.strip()
    total_cust = all_custs[all_custs != ''].nunique()
    
    return pd.Series({'meta_nmc': total_nmc, 'meta_flag': flag, 'meta_total_cust': total_cust})

purchase_meta = df.groupby('Реестровый номер извещения').apply(extract_purchase_meta)
df = df.merge(purchase_meta, left_on='Реестровый номер извещения', right_index=True, how='left')


# 4. ИСКЛЮЧЕНИЕ КОРНЕВЫХ ЛОТОВ
df_an = df[~df['_is_root']].copy()
print(f"   После исключения корневых лотов: {len(df_an)} строк")


# 5. РАСЧЁТ НМЦ
problems_list = []

def calculate_nmc_for_group(group):
    nmc_proc = group['meta_nmc'].iloc[0]
    unit_flag = group['meta_flag'].iloc[0]

    def run_allocation(kpgz_col):
        n_rows = len(group)
        unique_kpgz = group[kpgz_col].nunique()
        
        res_nmc = pd.Series(0.0, index=group.index)
        res_type = pd.Series('', index=group.index)
        
        if nmc_proc == 0:
            return res_nmc, res_type

        if unit_flag == 0:
            if n_rows == 1 and unique_kpgz == 1:
                nmc_spec = group['НМЦ спецификации'].iloc[0]
                res_nmc[:] = nmc_proc if nmc_spec != nmc_proc else nmc_spec
                res_type[:] = "A"
            elif unique_kpgz > 1:
                kpgz_spec_sums = group.groupby(kpgz_col)['НМЦ спецификации'].sum()
                total_spec_sum = kpgz_spec_sums.sum()
                if total_spec_sum > 0:
                    for kpgz in kpgz_spec_sums.index:
                        mask = group[kpgz_col] == kpgz
                        n_rows_k = mask.sum()
                        share = kpgz_spec_sums[kpgz] / total_spec_sum
                        res_nmc.loc[mask] = (share * nmc_proc) / n_rows_k
                        res_type.loc[mask] = "B"
                else:
                    problems_list.append(group.copy())
                    res_type[:] = "Ошибка B"
            else:
                res_nmc[:] = nmc_proc / n_rows
                res_type[:] = "C"
        else:
            if n_rows == 1 and unique_kpgz == 1:
                res_nmc[:] = nmc_proc
                res_type[:] = "D"
            else:
                X = group['НМЦ спецификации'].fillna(0).astype(float)
                C = group['НМЦ закупки'].fillna(0).astype(float)
                Y_lot = X.groupby(group['Реестровый номер лота']).transform('sum')
                Z = np.where(Y_lot > 0, X / Y_lot, 0.0)
                res_nmc = pd.Series(Z * C, index=group.index)
                res_type[:] = "E" if unique_kpgz > 1 else "F"
                
        return res_nmc, res_type

    nmc_3, type_3 = run_allocation('КПГЗ-3 (код)')
    nmc_k, type_k = run_allocation('КПГЗ конечный (код)')
    
    return pd.DataFrame({
        'Рассчитанная НМЦ': nmc_3,
        'Тип расчета НМЦ': type_3,
        'Рассчитанная НМЦ (кпгз конечный)': nmc_k,
        'Тип расчета НМЦ (кпгз конечный)': type_k
    })

print("⚙️ Расчёт НМЦ по методологии A-F...")
calc_res = df_an.groupby('Реестровый номер извещения', group_keys=False).apply(calculate_nmc_for_group)
df_an['Рассчитанная НМЦ'] = calc_res['Рассчитанная НМЦ']
df_an['Тип расчета НМЦ'] = calc_res['Тип расчета НМЦ']
df_an['Рассчитанная НМЦ (кпгз конечный)'] = calc_res['Рассчитанная НМЦ (кпгз конечный)']
df_an['Тип расчета НМЦ (кпгз конечный)'] = calc_res['Тип расчета НМЦ (кпгз конечный)']


# 6. ИСПРАВЛЕНИЕ ДУБЛИРУЮЩИХСЯ НАИМЕНОВАНИЙ
df_an.loc[df_an['КПГЗ конечный (код)'] == '01.02.10.14.13', 'КПГЗ конечный (наименование)'] = 'КОМПЛЕКТ/НАБОР ДЛЯ ВВЕДЕНИЯ ЛЕКАРСТВЕННЫХ ПРЕПАРАТОВ АМБУЛАТОРНЫЙ'
df_an.loc[df_an['КПГЗ конечный (код)'] == '01.02.10.35.55.04', 'КПГЗ конечный (наименование)'] = 'РАСХОДНЫЕ МАТЕРИАЛЫ К АППАРАТАМ ПОЧЕЧНОЙ ТЕРАПИИ'
df_an.loc[df_an['КПГЗ конечный (код)'] == '03.07.07.01.01.02', 'КПГЗ конечный (наименование)'] = 'ОБСЛУЖИВАНИЕ ТЕХНИЧЕСКОЕ И РЕМОНТ ПЕРВИЧНЫХ СРЕДСТВ ПОЖАРОТУШЕНИЯ'
print(f"   ✅ Исправлено наименований для 3 дублирующихся кодов КПГЗ конечный")


# 7. РАСЧЁТ ПРИЗНАКА ПЦП И КАТЕГОРИЙ
def calc_pcp_flag(row):
    method = str(row.get('Метод определения НМЦК', '')).strip()
    kpgz3 = str(row.get('КПГЗ-3 (код)', '')).strip()
    if method == 'Нормативный метод':
        return 'Да'
    elif method == 'Иной метод, предусмотренный законодательством' and kpgz3 == '03.10.01':
        return 'Да'
    return 'Нет'

df_an['Признак ПЦП'] = df_an.apply(calc_pcp_flag, axis=1)
df_an['Метод определения НМЦК'] = df_an['Метод определения НМЦК'].fillna('').astype(str)

def get_nmc_cat(val):
    if pd.isna(val) or val == 0: return "Без НМЦ"
    if val < 20_000_000: return "<20 млн"
    if val < 100_000_000: return "20-100 млн"
    if val < 500_000_000: return "100-500 млн"
    return ">500 млн"

df_an['Категория НМЦ'] = df_an['Рассчитанная НМЦ'].apply(get_nmc_cat)
df_an['Категория НМЦ (конечный)'] = df_an['Рассчитанная НМЦ (кпгз конечный)'].apply(get_nmc_cat)
df_an['Категория Конкуренции'] = pd.cut(df_an['Допущено участников'], bins=[-1, 0, 1, 2], labels=['0', '1', '2+'], right=False)
df_an['Категория Конкуренции'] = df_an['Категория Конкуренции'].fillna('0').astype(str)


# 8. ФУНКЦИЯ РАСЧЁТА ВСЕХ МЕТРИК
def calculate_all_metrics(data, code_col, name_col, nmc_col='Рассчитанная НМЦ', cat_col='Категория НМЦ', calc_pcp=False):
    nmc_cats = ['<20 млн', '20-100 млн', '100-500 млн', '>500 млн']
    comp_cats = ['0', '1', '2+']
    
    purchase_cols = ['Допущено участников', 'Подано заявок', 'Победитель наименование', 
                     'Победители ИНН', 'ГРБС']
    
    base_cols_for_unique = ['Реестровый номер извещения', code_col] + purchase_cols
    df_purch_base = data[base_cols_for_unique].drop_duplicates(subset=['Реестровый номер извещения', code_col])
    
    pcp_flag = data.groupby(['Реестровый номер извещения', code_col])['Признак ПЦП'].apply(lambda x: 'Да' if 'Да' in x.values else 'Нет').reset_index()
    df_purch_unique = df_purch_base.merge(pcp_flag, on=['Реестровый номер извещения', code_col], how='left')
    
    # Базовая статистика по лотам
    stats = data.groupby([code_col, name_col]).agg(
        Частота_лоты=('Реестровый номер лота', 'nunique'),
        Уникальные_ГРБС=('ГРБС', 'nunique'),
        Среднее_заявок_лоты=('Подано заявок', 'mean')
    ).reset_index()
    
    # Статистика по закупкам
    purchase_freq = df_purch_unique.groupby(code_col).size().reset_index(name='Частота_закупки')
    stats = stats.merge(purchase_freq, on=code_col, how='left')
    
    purchase_avg_bids = df_purch_unique.groupby(code_col)['Подано заявок'].mean().reset_index(name='Среднее_заявок_закупки')
    stats = stats.merge(purchase_avg_bids, on=code_col, how='left')
    
    # Несостоявшиеся закупки (0 или 1 участник или пусто)
    failed_mask = df_purch_unique['Допущено участников'].isin([0, 1]) | df_purch_unique['Допущено участников'].isna()
    failed_purch_count = df_purch_unique[failed_mask].groupby(code_col).size().reset_index(name='Несост_закупки')
    stats = stats.merge(failed_purch_count, on=code_col, how='left').fillna(0)
    stats['Несост_закупки'] = stats['Несост_закупки'].astype(int)
    stats['Доля несост закупок %'] = (stats['Несост_закупки'] / stats['Частота_закупки'] * 100).round(1)
    
    # Сумма НМЦ (по лотам)
    nmc_sum = data.groupby(code_col)[nmc_col].sum().reset_index(name='Сумма НМЦ')
    stats = stats.merge(nmc_sum, on=code_col, how='left')
    
    # Сумма несостоявшихся (по лотам, где Допущено = 0, 1 или пусто)
    failed_nmc = data[data['Допущено участников'].isin([0, 1]) | data['Допущено участников'].isna()]
    failed_nmc_sum = failed_nmc.groupby(code_col)[nmc_col].sum().reset_index(name='Сумма несостоявшихся')
    stats = stats.merge(failed_nmc_sum, on=code_col, how='left').fillna(0)
    
    # Уникальные победители (исключая "из контракта")
    mask_not_from_contract = ~df_purch_unique['Победитель наименование'].astype(str).str.contains('из контракта', case=False, na=False)
    winners_purch = df_purch_unique[df_purch_unique['Победитель наименование'].notna() & mask_not_from_contract]
    unique_winners = winners_purch.groupby(code_col)['Победитель наименование'].nunique().reset_index(name='УникПоб')
    stats = stats.merge(unique_winners, on=code_col, how='left').fillna(0)
    
    # Количество закупок С ПОБЕДИТЕЛЯМИ (исключая "из контракта")
    winners_total = winners_purch.groupby(code_col).size().reset_index(name='winners_total')
    stats = stats.merge(winners_total, on=code_col, how='left').fillna(0)
    stats['Доля уник победителей % (кол-во)'] = np.where(
        stats['winners_total'] > 0,
        (stats['УникПоб'] / stats['winners_total'] * 100).round(1),
        0.0
    )
    stats = stats.drop(columns=['winners_total'], errors='ignore')
    
    # ПЦП-блок
    if calc_pcp and 'Признак ПЦП' in data.columns:
        pcp_lot_count = data[data['Признак ПЦП'] == 'Да'].groupby(code_col).size().reset_index(name='Частота ПЦП_лоты')
        stats = stats.merge(pcp_lot_count, on=code_col, how='left').fillna(0)
        stats['Частота ПЦП_лоты'] = stats['Частота ПЦП_лоты'].astype(int)
        
        pcp_purch = df_purch_unique[df_purch_unique['Признак ПЦП'] == 'Да']
        pcp_purch_count = pcp_purch.groupby(code_col).size().reset_index(name='Частота ПЦП_закупки')
        stats = stats.merge(pcp_purch_count, on=code_col, how='left').fillna(0)
        stats['Частота ПЦП_закупки'] = stats['Частота ПЦП_закупки'].astype(int)
        stats['Признак ПЦП'] = stats['Частота ПЦП_закупки'].apply(lambda x: 'Да' if x > 0 else 'Нет')
        
        pcp_mask = data['Признак ПЦП'] == 'Да'
        pcp_sost = data[pcp_mask & (data['Допущено участников'] >= 2)].groupby(code_col)['Рассчитанная НМЦ (кпгз конечный)'].sum().reset_index(name='Сумма НМЦ ПЦП сост')
        pcp_nesost = data[pcp_mask & (data['Допущено участников'].isin([0, 1]) | data['Допущено участников'].isna())].groupby(code_col)['Рассчитанная НМЦ (кпгз конечный)'].sum().reset_index(name='Сумма НМЦ ПЦП несост')
        winner_mask = pcp_mask & data['Победитель наименование'].notna() & ~data['Победитель наименование'].astype(str).str.contains('из контракта', case=False, na=False)
        pcp_winner = data[winner_mask].groupby(code_col)['Рассчитанная НМЦ (кпгз конечный)'].sum().reset_index(name='НМЦ ПЦП с победителем')
        
        stats = stats.merge(pcp_sost, on=code_col, how='left').fillna(0)
        stats = stats.merge(pcp_nesost, on=code_col, how='left').fillna(0)
        stats = stats.merge(pcp_winner, on=code_col, how='left').fillna(0)
    
    # МАТРИЦА
    # Инициализация колонок
    for nmc in nmc_cats:
        for comp in comp_cats:
            stats[f'{nmc}_q_{comp}'] = 0
            stats[f'{nmc}_avg_{comp}'] = 0.0
            stats[f'{nmc}_нмц_{comp}'] = 0.0
    
    # АЛГОРИТМ: сначала фильтр по конкуренции, потом группировка по закупкам
    for comp in comp_cats:
        # Шаг 1: Фильтр по конкуренции на уровне ЛОТОВ
        if comp == '0':
            mask_comp = data['Допущено участников'].isin([0]) | data['Допущено участников'].isna()
        elif comp == '1':
            mask_comp = data['Допущено участников'] == 1
        else:  # 2+
            mask_comp = data['Допущено участников'] >= 2
        
        data_comp = data[mask_comp].copy()
        
        if data_comp.empty:
            continue
        
        # Шаг 2: Группируем по ЗАКУПКАМ и коду КПГЗ, суммируем НМЦ
        purch_nmc = data_comp.groupby(['Реестровый номер извещения', code_col])[nmc_col].sum().reset_index()
        
        # Шаг 3: Определяем категорию НМЦ для каждой закупки
        purch_nmc['nmc_cat'] = purch_nmc[nmc_col].apply(get_nmc_cat)
        
        # Шаг 4: Среднее заявок для этих закупок (из отфильтрованных данных)
        purch_bids = data_comp.groupby(['Реестровый номер извещения', code_col])['Подано заявок'].mean().reset_index()
        
        # Шаг 5: Для каждой категории НМЦ считаем метрики
        for nmc in nmc_cats:
            subset = purch_nmc[purch_nmc['nmc_cat'] == nmc]
            
            if subset.empty:
                continue
            
            # Количество уникальных закупок
            q_by_code = subset.groupby(code_col)['Реестровый номер извещения'].nunique()
            
            # Сумма НМЦ
            nmc_by_code = subset.groupby(code_col)[nmc_col].sum()
            
            # Среднее заявок для этих закупок
            subset_purch_list = subset[['Реестровый номер извещения', code_col]].drop_duplicates()
            subset_bids = purch_bids.merge(subset_purch_list, on=['Реестровый номер извещения', code_col], how='inner')
            avg_bids_by_code = subset_bids.groupby(code_col)['Подано заявок'].mean()
            
            # Заполняем в stats
            for code in q_by_code.index:
                mask = stats[code_col] == code
                stats.loc[mask, f'{nmc}_q_{comp}'] = q_by_code[code]
                stats.loc[mask, f'{nmc}_нмц_{comp}'] = nmc_by_code.get(code, 0)
                stats.loc[mask, f'{nmc}_avg_{comp}'] = round(avg_bids_by_code.get(code, 0), 2)
    
    # Доля состоявшихся (только 2+)
    stats['Доля сост закупок % (сумма НМЦ)'] = np.where(
        stats['Сумма НМЦ'] > 0,
        ((stats['<20 млн_нмц_2+'] + stats['20-100 млн_нмц_2+'] + stats['100-500 млн_нмц_2+'] + stats['>500 млн_нмц_2+']) / stats['Сумма НМЦ'] * 100).round(1),
        0.0
    )
    stats['Доля сост закупок % (кол-во)'] = np.where(
        stats['Частота_закупки'] > 0,
        ((stats['<20 млн_q_2+'] + stats['20-100 млн_q_2+'] + stats['100-500 млн_q_2+'] + stats['>500 млн_q_2+']) / stats['Частота_закупки'] * 100).round(1),
        0.0
    )
    
    # Переименование
    rename_dict = {
        'Частота_лоты': 'Частота (лоты)',
        'Частота_закупки': 'Частота (закупки)',
        'Среднее_заявок_лоты': 'Среднее_заявок (лоты)',
        'Среднее_заявок_закупки': 'Среднее_заявок (закупки)',
        'Несост_закупки': 'Несост закупки'
    }
    if calc_pcp:
        rename_dict['Частота ПЦП_лоты'] = 'Частота ПЦП (лоты)'
        rename_dict['Частота ПЦП_закупки'] = 'Частота ПЦП (закупки)'
    
    stats = stats.rename(columns=rename_dict)
    
    # Порядок колонок
    base_cols = [code_col, name_col, 'Частота (лоты)', 'Частота (закупки)', 
                 'Уникальные_ГРБС', 'Среднее_заявок (лоты)', 'Среднее_заявок (закупки)']
    
    if calc_pcp:
        base_cols.extend(['Частота ПЦП (лоты)', 'Частота ПЦП (закупки)', 'Признак ПЦП'])
    
    base_cols.extend(['Сумма НМЦ', 'Несост закупки', 'Доля несост закупок %', 'Сумма несостоявшихся', 
                      'Доля уник победителей % (кол-во)', 'Доля сост закупок % (сумма НМЦ)', 
                      'Доля сост закупок % (кол-во)'])
    
    if calc_pcp:
        base_cols.extend(['Сумма НМЦ ПЦП сост', 'Сумма НМЦ ПЦП несост', 'НМЦ ПЦП с победителем'])
    
    for nmc in nmc_cats:
        for comp in comp_cats:
            base_cols.extend([f'{nmc}_q_{comp}', f'{nmc}_avg_{comp}', f'{nmc}_нмц_{comp}'])
    
    final_cols = [col for col in base_cols if col in stats.columns]
    return stats[final_cols]


print("📊 Расчёт метрик...")
kpgz3_metrics = calculate_all_metrics(df_an, 'КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 
                                      nmc_col='Рассчитанная НМЦ', cat_col='Категория НМЦ', calc_pcp=False)

# ДОБАВЛЕНИЕ ПРИЗНАКА ПЦП ДЛЯ КПГЗ-3
# Логика: если хоть одна закупка по данному КПГЗ-3 имеет признак ПЦП = Да, то ставим "Да"
print("🏷️ Расчет признака ПЦП для КПГЗ-3...")
# 1. Определяем признак на уровне "Закупка + КПГЗ-3" (на случай если в закупке есть лоты с разными КПГЗ)
pcp_purch_kpgz3 = df_an.groupby(['Реестровый номер извещения', 'КПГЗ-3 (код)'])['Признак ПЦП'].apply(
    lambda x: 'Да' if 'Да' in x.values else 'Нет'
).reset_index()

# 2. Собираем список кодов КПГЗ-3, у которых есть хоть одна ПЦП-закупка
pcp_kpgz3_codes = pcp_purch_kpgz3[pcp_purch_kpgz3['Признак ПЦП'] == 'Да']['КПГЗ-3 (код)'].unique()

# 3. Присваиваем признак в сводную таблицу
kpgz3_metrics['Признак ПЦП'] = kpgz3_metrics['КПГЗ-3 (код)'].apply(lambda x: 'Да' if x in pcp_kpgz3_codes else 'Нет')

# 4. Перемещаем колонку после 'Среднее_заявок (закупки)' для красивого визуала
cols = kpgz3_metrics.columns.tolist()
if 'Среднее_заявок (закупки)' in cols and 'Признак ПЦП' in cols:
    idx = cols.index('Среднее_заявок (закупки)')
    cols.insert(idx + 1, cols.pop(cols.index('Признак ПЦП')))
    kpgz3_metrics = kpgz3_metrics[cols]

kpgz_final_metrics = calculate_all_metrics(df_an, 'КПГЗ конечный (код)', 'КПГЗ конечный (наименование)', 
                                           nmc_col='Рассчитанная НМЦ (кпгз конечный)', cat_col='Категория НМЦ (конечный)', calc_pcp=True)


# ДИАГНОСТИКА ДЛЯ КПГЗ-3 01.01.03
print("\n" + "="*80)
print("🔍 ДИАГНОСТИКА ДЛЯ КПГЗ-3: 01.01.03")
print("="*80)

test_kpgz = '01.01.03'

# 1. Данные из сводной таблицы
if test_kpgz in kpgz3_metrics['КПГЗ-3 (код)'].values:
    row = kpgz3_metrics[kpgz3_metrics['КПГЗ-3 (код)'] == test_kpgz].iloc[0]
    print(f"\n📊 Данные из сводной таблицы:")
    print(f"   Частота (закупки): {row['Частота (закупки)']}")
    print(f"   Несост закупки: {row['Несост закупки']}")
    print(f"\n   Матрица q:")
    for nmc in ['<20 млн', '20-100 млн', '100-500 млн', '>500 млн']:
        q0 = row[f'{nmc}_q_0']
        q1 = row[f'{nmc}_q_1']
        q2 = row[f'{nmc}_q_2+']
        if q0 + q1 + q2 > 0:
            print(f"      {nmc}: 0={q0}, 1={q1}, 2+={q2}")
    
    sum_q0 = sum(row[f'{nmc}_q_0'] for nmc in ['<20 млн', '20-100 млн', '100-500 млн', '>500 млн'])
    sum_q1 = sum(row[f'{nmc}_q_1'] for nmc in ['<20 млн', '20-100 млн', '100-500 млн', '>500 млн'])
    print(f"\n   Сумма _q_0: {sum_q0}")
    print(f"   Сумма _q_1: {sum_q1}")
    print(f"   Сумма _q_0 + _q_1: {sum_q0 + sum_q1}")
    print(f"   Несост закупки: {row['Несост закупки']}")
    if sum_q0 + sum_q1 != row['Несост закупки']:
        print(f"   ⚠️ Расхождение: {abs(sum_q0 + sum_q1 - row['Несост закупки'])}")
else:
    print(f"❌ КПГЗ {test_kpgz} не найден в сводной таблице")
    test_kpgz = None

# 2. Детальный разбор по закупкам
if test_kpgz:
    print(f"\n📋 ДЕТАЛЬНЫЙ РАЗБОР ПО ЗАКУПКАМ ДЛЯ КПГЗ {test_kpgz}:")
    
    # Берём данные из df_an
    kpgz_data = df_an[df_an['КПГЗ-3 (код)'] == test_kpgz].copy()
    
    # Сумма НМЦ по закупкам
    purch_nmc = kpgz_data.groupby('Реестровый номер извещения')['Рассчитанная НМЦ'].sum().reset_index()
    purch_nmc.columns = ['Реестровый номер извещения', 'total_nmc']
    purch_nmc['nmc_cat'] = purch_nmc['total_nmc'].apply(get_nmc_cat)
    
    # Уникальные закупки с допуском
    purch_unique = kpgz_data[['Реестровый номер извещения', 'Допущено участников']].drop_duplicates()
    purch_unique['comp_cat'] = pd.cut(purch_unique['Допущено участников'], bins=[-1, 0, 1, 2], labels=['0', '1', '2+'], right=False)
    purch_unique['comp_cat'] = purch_unique['comp_cat'].fillna('0').astype(str)
    
    # Объединяем
    purch_analysis = purch_unique.merge(purch_nmc, on='Реестровый номер извещения', how='left')
    
    print(f"\n   Всего уникальных закупок: {len(purch_analysis)}")
    print(f"\n   Распределение по категориям конкуренции:")
    print(purch_analysis['comp_cat'].value_counts().to_string())
    
    print(f"\n   Распределение по категориям НМЦ:")
    print(purch_analysis['nmc_cat'].value_counts().to_string())
    
    print(f"\n   Кросс-таблица (категория НМЦ x категория конкуренции):")
    cross = pd.crosstab(purch_analysis['nmc_cat'], purch_analysis['comp_cat'])
    print(cross.to_string())
    
    print(f"\n   Сумма по кросс-таблице:")
    print(f"      _q_0: {cross['0'].sum() if '0' in cross.columns else 0}")
    print(f"      _q_1: {cross['1'].sum() if '1' in cross.columns else 0}")
    print(f"      _q_0 + _q_1: {cross['0'].sum() if '0' in cross.columns else 0 + cross['1'].sum() if '1' in cross.columns else 0}")
    
    # Сравнение с Несост закупки
    failed_count = len(purch_analysis[purch_analysis['comp_cat'].isin(['0', '1'])])
    print(f"\n   Несост закупки (ручной подсчёт): {failed_count}")
    
    # Проверка дублирования закупок в df_purch_full
    print(f"\n🔍 ПРОВЕРКА ДУБЛИРОВАНИЯ В df_purch_full:")
    # Воспроизводим логику
    purchase_nmc_sum = df_an.groupby(['Реестровый номер извещения', 'КПГЗ-3 (код)'])['Рассчитанная НМЦ'].sum().reset_index(name='total_purchase_nmc')
    purchase_nmc_sum['cat_col_purch'] = purchase_nmc_sum['total_purchase_nmc'].apply(get_nmc_cat)
    
    # df_purch_unique для этого КПГЗ
    purchase_cols = ['Допущено участников', 'Подано заявок', 'Победитель наименование', 
                     'Победители ИНН', 'Категория НМЦ', 'Категория Конкуренции', 'ГРБС']
    base_cols = ['Реестровый номер извещения', 'КПГЗ-3 (код)'] + purchase_cols
    df_purch_base = df_an[base_cols].drop_duplicates(subset=['Реестровый номер извещения', 'КПГЗ-3 (код)'])
    df_purch_base = df_purch_base[df_purch_base['КПГЗ-3 (код)'] == test_kpgz]
    
    df_purch_full_test = df_purch_base.merge(
        purchase_nmc_sum[purchase_nmc_sum['КПГЗ-3 (код)'] == test_kpgz][['Реестровый номер извещения', 'КПГЗ-3 (код)', 'cat_col_purch', 'total_purchase_nmc']], 
        on=['Реестровый номер извещения', 'КПГЗ-3 (код)'], how='left'
    )
    
    print(f"   Количество строк в df_purch_full для этого КПГЗ: {len(df_purch_full_test)}")
    print(f"   Уникальных закупок: {df_purch_full_test['Реестровый номер извещения'].nunique()}")
    
    print(f"\n   Кросс-таблица из df_purch_full:")
    cross_full = pd.crosstab(df_purch_full_test['cat_col_purch'], df_purch_full_test['Категория Конкуренции'])
    print(cross_full.to_string())
    

# 9. СВЕРКИ ПО ЛИСТУ 1
print("\n" + "="*80)
print("🔍 СВЕРКИ ПО ЛИСТУ 1 (свод кпгз 3)")
print("="*80)

nmc_cols = [col for col in kpgz3_metrics.columns if '_нмц_' in col]
kpgz3_metrics['_sum_nmc_check'] = kpgz3_metrics[nmc_cols].sum(axis=1)
nmc_mismatch = kpgz3_metrics[abs(kpgz3_metrics['Сумма НМЦ'] - kpgz3_metrics['_sum_nmc_check']) > 0.01]
print(f"\n1. Сверка 'Сумма НМЦ' = сумма матричных НМЦ:")
print(f"   КПГЗ с расхождением: {len(nmc_mismatch)}")

nmc_0_cols = [col for col in kpgz3_metrics.columns if '_нмц_0' in col]
nmc_1_cols = [col for col in kpgz3_metrics.columns if '_нмц_1' in col]
kpgz3_metrics['_sum_failed_check'] = kpgz3_metrics[nmc_0_cols].sum(axis=1) + kpgz3_metrics[nmc_1_cols].sum(axis=1)
failed_mismatch = kpgz3_metrics[abs(kpgz3_metrics['Сумма несостоявшихся'] - kpgz3_metrics['_sum_failed_check']) > 0.01]
print(f"\n2. Сверка 'Сумма несостоявшихся' = сумма НМЦ_0 + НМЦ_1:")
print(f"   КПГЗ с расхождением: {len(failed_mismatch)}")

q_cols = [col for col in kpgz3_metrics.columns if '_q_' in col]
kpgz3_metrics['_sum_q_check'] = kpgz3_metrics[q_cols].sum(axis=1)
q_mismatch = kpgz3_metrics[abs(kpgz3_metrics['Частота (закупки)'] - kpgz3_metrics['_sum_q_check']) > 0.01]
print(f"\n3. Сверка 'Частота (закупки)' = сумма q-колонок:")
print(f"   КПГЗ с расхождением: {len(q_mismatch)}")

kpgz3_metrics = kpgz3_metrics.drop(columns=['_sum_nmc_check', '_sum_failed_check', '_sum_q_check'], errors='ignore')


# 10. ЛИСТ 3: свод кпгз конечный пцп
# ПЕРЕСЧИТЫВАЕМ МЕТРИКИ ТОЛЬКО ПО ПЦП ДАННЫМ (чтобы матрица была корректной)
pcp_data_for_metrics = df_an[df_an['Признак ПЦП'] == 'Да'].copy()
kpgz_final_pcp_metrics = calculate_all_metrics(pcp_data_for_metrics, 'КПГЗ конечный (код)', 'КПГЗ конечный (наименование)', 
                                               nmc_col='Рассчитанная НМЦ (кпгз конечный)', cat_col='Категория НМЦ (конечный)', calc_pcp=False)

print(f"   ✅ Сформирован лист 'свод кпгз конечный пцп': {len(kpgz_final_pcp_metrics)} строк")


# 11. ЛИСТ 4: свод пцп победители
print("📊 Формирование 'свод пцп победители'...")
# ИСПРАВЛЕНО: используем df_an, где есть колонка 'Признак ПЦП'
pcp_data = df_an[df_an['Признак ПЦП'] == 'Да'].copy()
pcp_data = pcp_data[pcp_data['Победители ИНН'].notna()]
pcp_data = pcp_data[~pcp_data['Победитель наименование'].astype(str).str.contains('из контракта', case=False, na=False)]

# Группировка по ИНН победителя
winners_pcp = pcp_data.groupby([
    'КПГЗ-3 (код)', 'КПГЗ-3 (наименование)',
    'КПГЗ конечный (код)', 'КПГЗ конечный (наименование)',
    'Признак ПЦП', 'Победители ИНН'
]).agg(
    Победитель_наименование=('Победитель наименование', 'first'),  # Первое наименование для ИНН
    НМЦ_рассчитанная_по_победителю=('Рассчитанная НМЦ (кпгз конечный)', 'sum'),
    частота_лоты_по_победителю=('Реестровый номер лота', 'nunique'),
    частота_закупки_по_победителю=('Реестровый номер извещения', 'nunique')
).reset_index()

# сумма_НМЦ_закупки — по уникальным закупкам (без дублирования лотов)
purch_nmc = pcp_data.drop_duplicates(subset=['Реестровый номер извещения', 'Победители ИНН', 'КПГЗ конечный (код)']).groupby(
    ['КПГЗ конечный (код)', 'Победители ИНН']
)['meta_nmc'].sum().reset_index(name='НМЦ_закупки_по_победителю')
winners_pcp = winners_pcp.merge(purch_nmc, on=['КПГЗ конечный (код)', 'Победители ИНН'], how='left')

# Кол-во уник победителей
total_unique_winners = winners_pcp.groupby(['КПГЗ конечный (код)'])['Победители ИНН'].nunique().reset_index(name='Кол-во уник победителей')
winners_pcp = winners_pcp.merge(total_unique_winners, on='КПГЗ конечный (код)', how='left')

winners_pcp = winners_pcp[[
    'КПГЗ-3 (код)', 'КПГЗ-3 (наименование)',
    'КПГЗ конечный (код)', 'КПГЗ конечный (наименование)',
    'Признак ПЦП', 'Кол-во уник победителей',
    'Победители ИНН', 'Победитель_наименование', 
    'НМЦ_закупки_по_победителю', 'НМЦ_рассчитанная_по_победителю', 
    'частота_лоты_по_победителю', 'частота_закупки_по_победителю'
]]

# Переименуем для соответствия оригинальному формату
winners_pcp = winners_pcp.rename(columns={'Победитель_наименование': 'Победитель наименование'})

print(f"   ✅ Сформирован лист 'свод пцп победители': {len(winners_pcp)} строк")


# 12. ЛИСТ 5: свод грбс
print("📊 Расчёт аналитики по ГРБС...")

# Формируем на основе df_an (уникальные лоты с разными КПГЗ)
df_grbs_src = df_an.copy()
df_grbs_src['ГРБС'] = df_grbs_src['ГРБС'].fillna('Не указан').astype(str)
df_grbs_src['Заказчик'] = df_grbs_src['Заказчик'].fillna('Не указан').astype(str)

# Важный момент: если ГРБС = "Правительство Москвы" и Заказчик начинается с "Департамент" или "префектура"
# то ГРБС = Заказчик
mask_gov_grbs = (df_grbs_src['ГРБС'] == 'Правительство Москвы') & (
    df_grbs_src['Заказчик'].str.lower().str.startswith('департамент') | 
    df_grbs_src['Заказчик'].str.lower().str.startswith('префектура')
)
df_grbs_src.loc[mask_gov_grbs, 'ГРБС'] = df_grbs_src.loc[mask_gov_grbs, 'Заказчик']

grp_kpgz_grbs = ['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'ГРБС']

def calc_grbs_metrics(group):
    # Уникальные закупки
    unique_purch = group['Реестровый номер извещения'].unique()
    n_purch = len(unique_purch)
    
    # НМЦ закупок - берем 1 раз на уникальную закупку
    nmc_purch_vals = group.drop_duplicates(subset='Реестровый номер извещения')['meta_nmc']
    sum_nmc_purch = nmc_purch_vals.sum()
    
    # НМЦ рассчитанная - суммируем по всем лотам
    sum_nmc_calc = group['Рассчитанная НМЦ'].sum()
    
    # Инициализация
    result = {
        'НМЦ закупок': sum_nmc_purch,
        'НМЦ рассчитанная': sum_nmc_calc,
        'Кол-во закупок': n_purch,
        'НМЦ_зак_0': 0.0,
        'НМЦ_зак_1': 0.0,
        'НМЦ_зак_2+': 0.0,
        'НМЦ_рас_0': 0.0,
        'НМЦ_рас_1': 0.0,
        'НМЦ_рас_2+': 0.0,
        'общее кол-во_0': 0,
        'общее кол-во_1': 0,
        'общее кол-во_2+': 0
    }
    
    # Для каждой категории конкуренции
    for comp in ['0', '1', '2+']:
        if comp == '0':
            mask_comp = group['Допущено участников'].isin([0]) | group['Допущено участников'].isna()
        elif comp == '1':
            mask_comp = group['Допущено участников'] == 1
        else:  # 2+
            mask_comp = group['Допущено участников'] >= 2
        
        group_comp = group[mask_comp]
        
        if group_comp.empty:
            continue
        
        # Уникальные закупки в этой категории
        unique_purch_comp = group_comp['Реестровый номер извещения'].unique()
        n_purch_comp = len(unique_purch_comp)
        
        # НМЦ закупок - 1 раз на уникальную закупку
        nmc_purch_comp = group_comp.drop_duplicates(subset='Реестровый номер извещения')['meta_nmc']
        sum_nmc_purch_comp = nmc_purch_comp.sum()
        
        # НМЦ рассчитанная - суммируем по всем лотам
        sum_nmc_calc_comp = group_comp['Рассчитанная НМЦ'].sum()
        
        result[f'НМЦ_зак_{comp}'] = sum_nmc_purch_comp
        result[f'НМЦ_рас_{comp}'] = sum_nmc_calc_comp
        result[f'общее кол-во_{comp}'] = n_purch_comp
    
    # Несостоявшиеся (0, 1 или пусто)
    result['НМЦ_зак_несост'] = result['НМЦ_зак_0'] + result['НМЦ_зак_1']
    result['НМЦ_рас_несост'] = result['НМЦ_рас_0'] + result['НМЦ_рас_1']
    result['Кол-во несост закупок'] = result['общее кол-во_0'] + result['общее кол-во_1']
    
    # Доли (ИСПРАВЛЕНО: используем round вместо .round())
    result['Доля несост закупок % (кол-во)'] = round(result['Кол-во несост закупок'] / n_purch * 100, 1) if n_purch > 0 else 0.0
    result['Доля сост закупок % (сумма НМЦ)'] = round(result['НМЦ_зак_2+'] / sum_nmc_purch * 100, 1) if sum_nmc_purch > 0 else 0.0
    result['Доля сост закупок % (кол-во)'] = round(result['общее кол-во_2+'] / n_purch * 100, 1) if n_purch > 0 else 0.0
    
    return pd.Series(result)

grbs_agg = df_grbs_src.groupby(grp_kpgz_grbs, group_keys=False).apply(calc_grbs_metrics).reset_index()

# Кол-во уник ГРБС на уровне КПГЗ
kpgz_lvl = df_grbs_src.drop_duplicates(subset=['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'ГРБС']).groupby(
    ['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)']
)['ГРБС'].nunique().reset_index(name='кол-во уник ГРБС')
grbs_agg = grbs_agg.merge(kpgz_lvl, on=['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)'], how='left')

# Порядок колонок
cols5 = ['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'кол-во уник ГРБС', 'ГРБС',
         'Доля сост закупок % (сумма НМЦ)', 'Доля сост закупок % (кол-во)',
         'НМЦ закупок', 'НМЦ рассчитанная', 'НМЦ_зак_несост', 'НМЦ_рас_несост',
         'Кол-во закупок', 'Кол-во несост закупок', 'Доля несост закупок % (кол-во)',
         'НМЦ_зак_0', 'НМЦ_зак_1', 'НМЦ_зак_2+',
         'НМЦ_рас_0', 'НМЦ_рас_1', 'НМЦ_рас_2+',
         'общее кол-во_0', 'общее кол-во_1', 'общее кол-во_2+']

df_grbs = grbs_agg[[c for c in cols5 if c in grbs_agg.columns]].copy()

print(f"   ✅ Сформирован лист 'свод грбс': {len(df_grbs)} строк")


# 13. ЛИСТ 6: свод грбс-заказчики
df_cust_src = df_an.copy()
df_cust_src['Заказчик'] = df_cust_src['Заказчик'].fillna('Не указан').astype(str)
df_cust_src['ГРБС'] = df_cust_src['ГРБС'].fillna('Не указан').astype(str)

# Важный момент: если ГРБС = "Правительство Москвы" и Заказчик начинается с "Департамент" или "префектура"
# то ГРБС = Заказчик
mask_gov = (df_cust_src['ГРБС'] == 'Правительство Москвы') & (
    df_cust_src['Заказчик'].str.lower().str.startswith('департамент') | 
    df_cust_src['Заказчик'].str.lower().str.startswith('префектура')
)
# меняем ГРБС на название Заказчика, чтобы ГРБС = Заказчик
df_cust_src.loc[mask_gov, 'ГРБС'] = df_cust_src.loc[mask_gov, 'Заказчик']

grp_cust = ['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'ГРБС', 'Заказчик']

def calc_cust_metrics(group):
    # Уникальные закупки
    unique_purch = group['Реестровый номер извещения'].unique()
    n_purch = len(unique_purch)
    
    # НМЦ - суммируем по всем лотам
    sum_nmc = group['Рассчитанная НМЦ'].sum()
    
    # Инициализация
    result = {
        'НМЦ': sum_nmc,
        'Кол-во закупок': n_purch,
        'НМЦ_0': 0.0,
        'НМЦ_1': 0.0,
        'НМЦ_2+': 0.0
    }
    
    # Для каждой категории конкуренции
    for comp in ['0', '1', '2+']:
        if comp == '0':
            mask_comp = group['Допущено участников'].isin([0]) | group['Допущено участников'].isna()
        elif comp == '1':
            mask_comp = group['Допущено участников'] == 1
        else:  # 2+
            mask_comp = group['Допущено участников'] >= 2
        
        group_comp = group[mask_comp]
        
        if group_comp.empty:
            continue
        
        # НМЦ рассчитанная - суммируем по всем лотам
        sum_nmc_comp = group_comp['Рассчитанная НМЦ'].sum()
        result[f'НМЦ_{comp}'] = sum_nmc_comp
    
    # Несостоявшиеся (0, 1 или пусто)
    result['НМЦ несост заказчика'] = result['НМЦ_0'] + result['НМЦ_1']
    result['НМЦ сост заказчика'] = result['НМЦ_2+']
    
    # Несост по кол-ву (0, 1 или пусто)
    mask_nesost = group['Допущено участников'].isin([0, 1]) | group['Допущено участников'].isna()
    nesost_purch = group[mask_nesost]['Реестровый номер извещения'].unique()
    n_nesost = len(nesost_purch)
    
    # Доли (ИСПРАВЛЕНО: используем round вместо .round())
    result['Доля несост закупок % (кол-во)'] = round(n_nesost / n_purch * 100, 1) if n_purch > 0 else 0.0
    result['Доля несост закупок % (сумма НМЦ)'] = round(result['НМЦ_0'] / sum_nmc * 100, 1) if sum_nmc > 0 else 0.0
    
    return pd.Series(result)

grbs_cust = df_cust_src.groupby(grp_cust, group_keys=False).apply(calc_cust_metrics).reset_index()

# Кол-во уникальных заказчиков
cust_cnt = df_cust_src.drop_duplicates(subset=['КПГЗ-3 (код)', 'ГРБС', 'Заказчик']).groupby(
    ['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'ГРБС']
)['Заказчик'].nunique().reset_index(name='кол-во уникальных заказчиков')
grbs_cust = grbs_cust.merge(cust_cnt, on=['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'ГРБС'], how='left')

cols6 = ['КПГЗ-3 (код)', 'КПГЗ-3 (наименование)', 'ГРБС', 'НМЦ', 'кол-во уникальных заказчиков',
         'Доля несост закупок % (кол-во)', 'Доля несост закупок % (сумма НМЦ)',
         'Кол-во закупок', 'Заказчик', 'НМЦ несост заказчика', 'НМЦ сост заказчика',
         'НМЦ_0', 'НМЦ_1', 'НМЦ_2+']
df_grbs_cust = grbs_cust[[c for c in cols6 if c in grbs_cust.columns]].copy()

print(f"   ✅ Сформирован лист 'свод грбс-заказчики': {len(df_grbs_cust)} строк")


# 14. ВЫГРУЗКА
vygruz = True  # True для сохранения

if vygruz:
    detail_cols = [
        'Реестровый номер извещения', 'Реестровый номер лота',
        'КПГЗ-3 (код)', 'КПГЗ-3 (наименование)',
        'КПГЗ конечный (код)', 'КПГЗ конечный (наименование)',
        'НМЦ закупки', 'НМЦ спецификации',
        'Рассчитанная НМЦ', 'Тип расчета НМЦ',
        'Рассчитанная НМЦ (кпгз конечный)', 'Тип расчета НМЦ (кпгз конечный)',
        'Подано заявок', 'Допущено участников',
        'ГРБС', 'Заказчик', 'Победитель наименование',
        'Признак ПЦП', 'Метод определения НМЦК'
    ]
    
    df_export = df_an[[c for c in detail_cols if c in df_an.columns]].copy()
    
    detail_file = os.path.join(out_dir, f"кпгз_детали_{ts}.xlsx")
    with pd.ExcelWriter(detail_file, engine='openpyxl') as writer:
        df_export.to_excel(writer, index=False)
    
    summary_file = os.path.join(out_dir, f"кпгз_свод_{ts}.xlsx")
    with pd.ExcelWriter(summary_file, engine='openpyxl') as writer:
        kpgz3_metrics.to_excel(writer, sheet_name="свод кпгз 3", index=False)
        kpgz_final_metrics.to_excel(writer, sheet_name="свод кпгз конечный", index=False)
        kpgz_final_pcp_metrics.to_excel(writer, sheet_name="свод кпгз конечный пцп", index=False)
        winners_pcp.to_excel(writer, sheet_name="свод пцп победители", index=False)
        df_grbs.to_excel(writer, sheet_name="свод грбс", index=False)
        df_grbs_cust.to_excel(writer, sheet_name="свод грбс-заказчики", index=False)
        
        if problems_list:
            prob_df = pd.concat(problems_list).drop_duplicates()
            prob_df.to_excel(writer, sheet_name="нмц проблемы", index=False)
    
    print(f"\n💾 Файлы сохранены в: {out_dir}")

print("\n✅ АНАЛИЗ ЗАВЕРШЁН.")
