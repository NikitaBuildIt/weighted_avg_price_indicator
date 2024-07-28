import flet as ft
import asyncio
import time
from websocket_api import start_streams, get_avg_price
import multiprocessing
import random
import json
from datetime import datetime
from loguru import logger
import os, sys
import threading
import queue

# Глобальные переменные для хранения вкладок и виджета Tabs
tabs = []
tab_view = None
worker = None

stop_event = threading.Event()
data_queue = queue.Queue()


def main(page: ft.Page):
    global tab_view, tabs, worker, stop_event, data_queue

    page.title = 'AVG Weighted Price Indicator'
    page.theme_mode = 'light'

    # Глобальные переменные для хранения значений из полей
    pair_field = None
    period_field = None

    def set_text_upper(e):
        new_text = e.control.value.upper()
        e.control.value = new_text
        page.update()

    def remove_tab(pair_text):
        global tab_view, tabs
        tabs = [tab for tab in tabs if tab.text != pair_text]  # Удаление вкладки по индексу
        tab_view.tabs = tabs  # Обновление вкладок в виджете Tabs
        page.update()  # Обновление страницы

    def show_period_container_true(e):
        if pair_field.value or period_field.value:
            avg_per_period.value = asyncio.run(get_avg_price(pair=str(pair_field.value), days=int(period_field.value)))
            period_container.visible = True
            page.update()  # Обновление страницы, чтобы отобразить изменения
        else:
            return

    def show_period_container_false(e):
        avg_per_period.value = 0
        period_container.visible = False
        page.update()  # Обновление страницы, чтобы отобразить изменения

    def update_current_avg():
        ticks = 0
        while True:
            ticks += 1
            avg_prices = data_queue.get()

            tasks = []
            for tab in tabs:
                if tab.text != "Главная" and tab.text in avg_prices:  # Проверяем, что это не главная вкладка
                    try:
                        # Получаем значения из вкладки
                        pair_text = (tab.content.content.controls[0].controls[0].value).replace('Пара: ', '')

                        update_avg_for_tab(tab, avg_prices[pair_text], ticks)
                    except Exception as e:
                        logger.info(f"Ошибка при обработке вкладки: {e}")

                        # Выполняем все задачи параллельно


            # Обновляем весь виджет Tabs и страницу
            tab_view.update()
            time.sleep(1)

    def update_avg_for_tab(tab, avg_price, ticks):
        try:
            text_field = tab.content.content.controls[1].controls[1]
            text_field.value = 'Текущая средняя: ' + str(avg_price)
            text_field.update()

            if ticks % 60 == 0 or ticks < 3:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                line_chart = tab.content.content.controls[1].controls[0]
                line_chart.data_series[0].data_points.append(
                    ft.LineChartDataPoint(x=len(line_chart.data_series[0].data_points), y=avg_price,
                                          tooltip=f"Время: {now}, Значение: {avg_price}"))
                line_chart.data_series[0].point = True

                all_y_values = [point.y for point in line_chart.data_series[0].data_points]

                if all_y_values:
                    max_y = max(all_y_values) * 2  # Немного выше максимального значения

                    if line_chart.max_y != max_y:
                        line_chart.max_y = max_y
                        line_chart.update()

                # Обновляем график
                line_chart.update()
        except Exception as e:
            logger.info(f"Ошибка при обновлении средней цены для вкладки {tab.text}: {e}")

    def start_ws(tabs, stop_event, data_queue, days):
        pairs_list = []
        for pair in tabs[1:]:
            pair = pair.text
            pairs_list.append(pair)
        asyncio.run(start_streams(pairs_list, stop_event, data_queue, days))

    def add_tab(e):
        global tab_view, tabs, worker, stop_event, data_queue

        pair_text = pair_field.value
        period_text = period_field.value
        if not period_text or not period_field: return

        show_period_container_false(None)
        # Получение значений из полей

        line = ft.LineChartData(
            color=ft.colors.GREEN,
            stroke_width=5,
            curved=True,
            stroke_cap_round=True,
            below_line_gradient=ft.LinearGradient(
                begin=ft.alignment.top_center,
                end=ft.alignment.bottom_center,
                colors=[
                    ft.colors.with_opacity(0.25, ft.colors.GREEN),
                    "transparent"
                ]
            ),
            data_points=[]
        )
        new_tab = ft.Tab(
            text=f"{pair_text}",
            content=ft.Container(
                content=ft.Row(
                    controls=[
                        ft.Column(
                            controls=[
                                ft.Text(f"Пара: {pair_text}", size=20, font_family='Montserrat'),
                                ft.Text(f"Период: {period_text}", size=20, font_family='Montserrat'),
                                ft.ElevatedButton(
                                    text="Удалить",
                                    width=250,
                                    height=50,
                                    bgcolor=ft.colors.RED,  # Установка цвета фона кнопки
                                    color=ft.colors.WHITE,  # Установка цвета текста кнопки
                                    on_click=lambda e: remove_tab(pair_text)  # Передача текста пары для удаления
                                )
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            spacing=15,
                        ),
                        ft.Column(
                            controls=[
                                ft.LineChart(
                                    data_series=[line],
                                    border=ft.border.all(3, ft.colors.with_opacity(0.2, ft.colors.ON_SURFACE)),
                                    height=400,
                                    width=920,
                                    min_y=0, max_y=0
                                ),
                                ft.Text(f"Текущая средняя: 0", size=20, font_family='Montserrat', selectable=True),

                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            spacing=15
                        )
                    ],
                    alignment=ft.MainAxisAlignment.START,
                    spacing=20  # Расстояние между колонками
                ),
                alignment=ft.alignment.top_left,
                padding=20
            ),
        )

        tabs.append(new_tab)  # Добавление новой вкладки в список
        tab_view.tabs = tabs  # Обновление вкладок в виджете Tabs
        pair_field.value = ""  # Очистка поля ввода для пары
        period_field.value = ""  # Очистка поля ввода для периода
        page.update()  # Обновление страницы

        if worker and worker.is_alive():
            stop_event.set()
            worker.join()

        if period_text:
            stop_event.clear()
            worker = threading.Thread(target=start_ws, args=(tabs, stop_event, data_queue, int(period_text)), daemon=True)
            worker.start()

    # Инициализация вкладок с кнопкой для добавления новых вкладок
    pair_field = ft.TextField(label="Пара", on_change=set_text_upper, width=350, height=50,
                              on_focus=show_period_container_false)
    period_field = ft.TextField(label="Период(дней)", on_change=set_text_upper, width=350, height=50,
                                on_focus=show_period_container_false)

    avg_per_period = ft.Text("0", size=40, font_family='Montserrat', weight=ft.FontWeight.BOLD, selectable=True)
    # Контейнер для текста, который изначально невидим
    period_container = ft.Container(
        content=ft.Column(
            controls=[ft.Text("Средняя взвешенная цена:", size=20, font_family='Montserrat'),
                      avg_per_period],

            horizontal_alignment=ft.CrossAxisAlignment.CENTER
        ),
        visible=False,  # Установка начальной видимости как False
        padding=160,
        width=900,
    )

    tabs = [
        ft.Tab(
            text="Главная",
            content=ft.Container(
                content=ft.Row(
                    controls=[
                        ft.Column(
                            controls=[
                                ft.Text("Добавить пару для отслеживания", size=20, font_family='Montserrat'),
                                pair_field,
                                period_field,
                                ft.ElevatedButton(
                                    text="Отслеживать среднюю",
                                    width=250,
                                    height=50,
                                    on_click=add_tab  # Передача функции без вызова
                                ),
                                ft.ElevatedButton(
                                    text="Средняя за период",
                                    width=250,
                                    height=50,
                                    on_click=show_period_container_true  # Передача функции без вызова
                                ),
                            ],
                            alignment=ft.MainAxisAlignment.CENTER,
                            horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                            spacing=15
                        ),
                        period_container  # Добавление невидимого контейнера справа
                    ],
                    alignment=ft.MainAxisAlignment.START,
                    spacing=20  # Расстояние между основным содержимым и контейнером
                ),
                alignment=ft.alignment.top_left,
                padding=20
            ),
        )
    ]

    # Создание виджета Tabs
    tab_view = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        tabs=tabs,
        expand=1,
    )

    # Добавление вкладок на страницу
    page.add(tab_view)

    threading.Thread(target=update_current_avg, daemon=True).start()


if __name__ == "__main__":
    logger.add("logs.log", rotation="1 week", retention="1 month", level="INFO")
    ft.app(target=main)

