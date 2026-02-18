import streamlit as st
import pandas as pd
import io

# настройка страницы

st.set_page_config(layout="wide", page_title="Анализ контрактов и лотов")

# загрузка данных

@st.cache_data
def load_data():
    try:
        contracts = pd.read_excel(
            "Контракты 2025_2025-12-15.xlsx", 
            sheet_name=1,
            dtype={
            "номер контакта в ЕИС": str,
            "Реестровый номер извещения ЕИС": str,
            "Реестровый номер в РК": str,
            "ID": str,
            },
            engine='openpyxl'
        )
        lots = pd.read_excel(
            "Лоты 2025_2025-12-15.xlsx", 
            sheet_name=1,
            dtype={
            "Иден. код закупки (ИКЗ)": str,
            "Реес. номер лота в план. ЕАИСТ": str,
            "Реестровый номер извещения ЕИС": str,
            "LVID": str,
            "ID": str,
            },
            engine='openpyxl'
        )
        return contracts, lots
    except Exception as e:
        st.error(f"Ошибка загрузки файлов: {e}")
        st.stop()

contracts, lots = load_data()

# выбор таблицы

table_choice = st.radio("Выберите выгрузку:", ["Контракты", "Лоты"], horizontal=True)
df = contracts.copy() if table_choice == "Контракты" else lots.copy()

# преобразование типов

if table_choice == "Контракты":
    date_cols = ["Дата ок. ср действия контр.", "Дата заключения", "Дата регистрации", "Срок исполнения с", "Срок исполнения по"]
    numeric_cols = [
        "Цена ГК, руб.", "PURCHASE_SUM_2024", "PURCHASE_SUM_2025", "PURCHASE_SUM_2026", "PURCHASE_SUM_2027",
        "Цена ГК при заключении, руб.", "Оплачено, руб", "Оплачено, %"
    ]
else:
    date_cols = [
        "START_DATE", "Дата публикации извещения", "Дата итогового протокола",
        "PLANNED_CONTRACT_DATE", "Фактическая дата пуб. лота", "Дата 1 вкл в ПГ",
        "Дата изм", "Дата первой пуб проц"
    ]
    numeric_cols = [
        "NMC_SUM", "PUBLISHED_IN_PLAN_NMC", "НМЦ проект", "Цена контракта при заключении",
        "Цена победителя", "NMC_SUMMA", "Совместная закупка", "Кол-во поданных заявок",
        "Кол-во доп по 1ым частям", "Кол-во допущенных участников", "COST_IN_RUBLE",
        "процент среди СМП", "сумма по СМП", "Предлагаемая цена",
        "Проверка", "Экономия проект", "Снижение НМЦ", "Снижение %"
    ]

# приведение типов

for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(
            df[col], 
            dayfirst=True,
            errors='coerce')
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(
            df[col],
            errors='coerce')

# выбор колонок для отображения

all_columns = df.columns.tolist()
selected_columns = st.multiselect(
    "Выберите колонки для отображения:",
    options=all_columns,
    default=all_columns[:min(5, len(all_columns))]
)

if not selected_columns:
    st.info("Выберите хотя бы одну колонку для отображения и фильтрации.")
    st.stop()

# конфигурация типов колонок

COLUMN_CONFIG = {
    "Контракты": {
        "cat": [
            "CONTRACT20", "ID", "Реестровый номер в РК", "номер контакта в ЕИС",
            "Реестровый номер извещения ЕИС", "Заказчик", "ИНН", "ГРБС",
            "Наименование (предмет) ГК", "Номер лота в закупке", "Основание заключения",
            "Статус ГК", "№ версии", "Место участника", "Наименование поставщика",
            "ИНН Поставщика", "КПП Поставщика", "Субъект РФ поставщика",
            "Наименование субъекта", "Закон-основание (Лоты)", "Закон-основание (Контракты)",
            "METHOD_OF_SUPPLIER", "REASON_METHOD_OF_SUPPLIER_ID", "LOT_ID",
            "Реестр. ном. лота", "Закон осн. договора (223ФЗ)", "Состояние"
        ],
        "num": [
            "Цена ГК, руб.", "PURCHASE_SUM_2024", "PURCHASE_SUM_2025", "PURCHASE_SUM_2026", "PURCHASE_SUM_2027",
            "Цена ГК при заключении, руб.", "Оплачено, руб", "Оплачено, %"
        ],
        "date": [
            "Дата ок. ср действия контр.", "Дата заключения", "Дата регистрации",
            "Срок исполнения с", "Срок исполнения по"
        ],
        "kpgz_pairs": [("КПГЗ 2", "КПГЗ 2 Наименование"), ("КПГЗ 3", "КПГЗ 3 Наименование")]
    },
    "Лоты": {
        "cat": [
            "LVID", "Иден. код закупки (ИКЗ)", "Реес. номер лота в план. ЕАИСТ",
            "Реестровый номер извещения ЕИС", "Номер лота в закупке",
            "Уровень осуществления закупки", "Способ определения поставщика",
            "Главный заказчик закупки", "Уполномоченная организация", "Заказчик",
            "ИНН Заказчик", "ИНН Организатора", "ГРБС", "Комплекс", "Наименование лота",
            "LOT_STATUS", "SMP_TYPE", "COL", "КПГЗ (если нет КПГЗ 3)", "ЭТП",
            "CNT_VERSION", "IS_AUTO_GENERATED", "Наименование победителя",
            "ИНН Победителя", "Ид контракта", "Контракт", "Место участника",
            "JOINT_AUCTION", "LEENTITY_IDLEENTITY_ID", "Номер контракта", "ФЗ",
            "Прием. орг. инвалидов", "Прием. учереж. УИС", "Иностран. товары",
            "Метод определения НМЦК", "ID"
        ],
        "num": [
            "NMC_SUM", "PUBLISHED_IN_PLAN_NMC", "НМЦ проект", "Цена контракта при заключении",
            "Цена победителя", "NMC_SUMMA", "Совместная закупка", "Кол-во поданных заявок",
            "Кол-во доп по 1ым частям", "Кол-во допущенных участников", "COST_IN_RUBLE",
            "процент среди СМП", "сумма по СМП", "Предлагаемая цена",
            "Проверка", "Экономия проект", "Снижение НМЦ", "Снижение %"
        ],
        "date": [
            "START_DATE", "Дата публикации извещения", "Дата итогового протокола",
            "PLANNED_CONTRACT_DATE", "Фактическая дата пуб. лота", "Дата 1 вкл в ПГ",
            "Дата изм", "Дата первой пуб проц"
        ],
        "kpgz_pairs": [("КПГЗ 2", "КПГЗ 2 Наименование"), ("КПГЗ 3", "КПГЗ 3 Наименование")]
    }
}

config = COLUMN_CONFIG[table_choice]

# фильтрация данных

conditions = []  # список масок (булевых условий)
applied_kpgz_cols = set()

config = COLUMN_CONFIG[table_choice]

# взаимоисключающие КПГЗ

for col_a, col_b in config["kpgz_pairs"]:
    available = [c for c in [col_a, col_b] if c in selected_columns]
    if len(available) == 2:
        choice = st.radio(
            f"Фильтр для КПГЗ (выберите одно):",
            options=available,
            horizontal=True,
            key=f"kpgz_{col_a}"
        )
        available = [choice]
    for col in available:
        if col in df.columns:
            unique_vals = sorted(df[col].dropna().astype(str).unique())
            sel = st.multiselect(f"FilterWhere: {col}", unique_vals)
            if sel:
                conditions.append(df[col].astype(str).isin(sel))
            applied_kpgz_cols.add(col)

# остальные колонки

other_cols = [c for c in selected_columns if c not in applied_kpgz_cols]
cat_cols = [c for c in other_cols if c in config["cat"]]
num_cols = [c for c in other_cols if c in config["num"]]
date_cols = [c for c in other_cols if c in config["date"]]

# функция для отображения фильтров по 2 шт на 1 строку

def render_in_pairs(columns, render_func):
    for i in range(0, len(columns), 2):
        cols_ui = st.columns(min(2, len(columns) - i))
        for j, col in enumerate(columns[i:i+2]):
            with cols_ui[j]:
                render_func(col, conditions)

# текстовые фильтры

if cat_cols:
    st.markdown("#### Текстовые фильтры")
    def render_cat(col, conds):
        unique_vals = sorted(df[col].dropna().astype(str).unique())
        sel = st.multiselect(f"Выбор значения: {col}", unique_vals)
        if sel:
            conds.append(df[col].astype(str).isin(sel))
    render_in_pairs(cat_cols, render_cat)

# числовые фильтры

if num_cols:
    st.markdown("#### Числовые диапазоны")
    def render_num(col, conds):
        min_val_orig = df[col].min()
        max_val_orig = df[col].max()
        min_val = float(min_val_orig) if pd.notna(min_val_orig) else 0.0
        max_val = float(max_val_orig) if pd.notna(max_val_orig) else 0.0
        st.caption(f"{col}: минимум = {min_val:,.2f} , максимум = {max_val:,.2f}")
        col1, col2 = st.columns(2)
        with col1:
            low = st.number_input(f"Мин ({col})", value=min_val, min_value=0.0, step=1000.0, key=f"min_{col}")
        with col2:
            high = st.number_input(f"Макс ({col})", value=max_val, min_value=0.0, step=1000.0, key=f"max_{col}")
        conds.append((df[col] >= low) & (df[col] <= high))
    render_in_pairs(num_cols, render_num)

# дата-фильтры (обычные)

other_date_cols = [c for c in date_cols if c not in ["Срок исполнения с", "Срок исполнения по"]]

if other_date_cols:
    st.markdown("#### Фильтры по датам")
    def render_date(col, conds):
        min_d = df[col].min()
        max_d = df[col].max()
        if pd.notna(min_d) and pd.notna(max_d):
            st.caption(f"{col}: от {min_d.date()} до {max_d.date()}")
            d1 = st.date_input(f"От ({col})", value=min_d.date(), key=f"from_{col}")
            d2 = st.date_input(f"До ({col})", value=max_d.date(), key=f"to_{col}")
            conds.append((df[col].dt.date >= d1) & (df[col].dt.date <= d2))
    render_in_pairs(other_date_cols, render_date)

# "Срок исполнения с" и "Срок исполнения по"

if table_choice == "Контракты":
    col_start = "Срок исполнения с"
    col_end = "Срок исполнения по"

    # проверяем, выбраны ли обе колонки

    show_start = col_start in selected_columns
    show_end = col_end in selected_columns

    if show_start or show_end:
        st.markdown("#### Срок исполнения контракта")

        # получаем мин/макс для подсказок

        min_start = df[col_start].min().date() if pd.notna(df[col_start].min()) else None
        max_start = df[col_start].max().date() if pd.notna(df[col_start].max()) else None
        min_end = df[col_end].min().date() if pd.notna(df[col_end].min()) else None
        max_end = df[col_end].max().date() if pd.notna(df[col_end].max()) else None

        # выводим поля ввода

        cols_ui = st.columns(2)
        with cols_ui[0]:
            if show_start:
                if min_start and max_start:
                    st.caption(f"{col_start}: от {min_start} до {max_start}")
                    start_date = st.date_input(col_start, value=min_start, key="exec_from")
                else:
                    start_date = st.date_input(col_start, key="exec_from")
            else:
                start_date = None

        with cols_ui[1]:
            if show_end:
                if min_end and max_end:
                    st.caption(f"{col_end}: от {min_end} до {max_end}")
                    end_date = st.date_input(col_end, value=max_end, key="exec_to")
                else:
                    end_date = st.date_input(col_end, key="exec_to")
            else:
                end_date = None

        # валидация

        if start_date and end_date and start_date > end_date:
            st.error('"Срок исполнения с" не может быть позже "Срока исполнения по"!')
        else:

            # арименяем фильтры

            if start_date:
                conditions.append(df[col_start].dt.date >= start_date)
            if end_date:
                conditions.append(df[col_end].dt.date <= end_date)

# применяем все условия

if conditions:
    final_mask = conditions[0]
    for cond in conditions[1:]:
        final_mask &= cond
    filtered_df = df[final_mask]
else:
    filtered_df = df

# отображение результата

display_df = filtered_df[selected_columns] if selected_columns else filtered_df

st.write(f"Результат выбора: {len(display_df)}")
st.dataframe(display_df, use_container_width=True)

# кнопки для скачивания

# CSV 

csv = display_df.to_csv(index=False, encoding='utf-8-sig')
st.download_button(
    label="Скачать как CSV",
    data=csv,
    file_name="filtered_data.csv",
    mime="text/csv"
)

# XLSX

output = io.BytesIO()
with pd.ExcelWriter(output, engine='openpyxl') as writer:
    display_df.to_excel(writer, index=False, sheet_name='Результат')
    # Применяем форматирование для ID-колонок
    worksheet = writer.sheets['Результат']
    for col_num, col_name in enumerate(display_df.columns, 1):
        if col_name in ["номер контакта в ЕИС", "Реестровый номер извещения ЕИС", "ID", "LVID", "Иден. код закупки (ИКЗ)", "Реес. номер лота в план. ЕАИСТ"]:
            for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row, min_col=col_num, max_col=col_num):
                for cell in row:
                    cell.number_format = '@'  # текстовый формат

st.download_button(
    label="Скачать как XLSX",
    data=output.getvalue(),
    file_name="filtered_data.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
)
