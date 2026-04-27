#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import time
from datetime import datetime
from confluent_kafka import Consumer, KafkaException, TopicPartition
from database import Database
from parser import parse_message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('kafka_consumer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class KafkaConsumer:
    
    def __init__(self):
        self.consumer = None
        self.db = None
        self.running = True
        
        # CDC топики для подписки
        
        self.cdc_topics = [
            '...cdcData.mysystem.contract.v-1',
            '...cdcData.mysystem.lot.v-1',
            '...cdcData.mysystem.procedure.v-1',
            '...cdcData.mysystem.purchasePlan.v-1',
            '...cdcData.mysystem.planSchedule.v-1',
            # ...
        ]
    
    
    def initialize(self):
        
        """Инициализация подключений"""
      
        logger.info("Инициализация Kafka Consumer...")
        
        # Подключение к Kafka
        
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka1...:1111,kafka2...:1111',
            'security.protocol': '...',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': '...',
            'sasl.password': '...',
            'ssl.ca.location': '/...',
            'ssl.certificate.location': '/...',
            'ssl.key.location': '/...',
            'group.id': 'cdc_consumer_group',
            'auto.offset.reset': 'latest', # новые сообщения
            'enable.auto.commit': False, # ручной коммит
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000
        })
        
        # Подписка на CDC топики
        
        self.consumer.subscribe(self.cdc_topics)
        logger.info(f" Подписано на CDC топики: {len(self.cdc_topics)}")
        
        # Подключение к БД
        
        self.db = Database({
            'host': 'localhost',
            'port': 1111,
            'database': 'postgres',
            'user': 'postgres',
            'password': '1111'
        })
        logger.info(" Подключено к PostgreSQL")
    
    
    def process_message(self, msg):
        
        """Обработка одного CDC-сообщения"""
        
        try:
            
            # Парсинг JSON
            
            value = msg.value().decode('utf-8')
            data = json.loads(value)
            
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            
            logger.debug(f"Получено сообщение: topic={topic}, partition={partition}, offset={offset}")
            
            # Проверка статуса
            
            if data.get('status') != 'OK':
                logger.warning(f"Пакет со статусом ERROR: {data.get('error')}")
                
                self.consumer.commit(msg)
                return
            
            # Проверка типа данных
            
            data_type = data.get('dataType')
            if data_type not in ['CdcData', 'CdcMsg']:
                logger.warning(f"Неизвестный тип данных: {data_type}")
                self.consumer.commit(msg)
                return
            
            object_type = data.get('objectType')
            body = data.get('body')
            
            # Если CdcMsg - только уведомление, нужно делать дополнительный запрос
            # Если CdcData - полные данные в body
            
            if data_type == 'CdcData' and body:
                
                # Парсим и сохраняем
                
                parsed_data = parse_message(data)
                
                if parsed_data:
                    
                    # Сохраняем в БД с UPSERT
                    
                    self.db.save_message(parsed_data)
                    
                    logger.info(f" Обработано: objectType={object_type}, "
                              f"id={body.get('id')}, entityId={body.get('entityId')}")
                else:
                    logger.warning(f"Парсер вернул None для objectType={object_type}")
            else:
                logger.info(f" ! Получено уведомление об изменении (CdcMsg): "
                          f"objectType={object_type}, id={body.get('id') if body else 'N/A'}")
                
                # Здесь можно будет сделать доп HTTP-запрос для получения полных данных
            
            # Коммит оффсета ТОЛЬКО после успешной обработки
            
            self.consumer.commit(msg)
            
        except json.JSONDecodeError as e:
            logger.error(f"✗ Ошибка парсинга JSON: {e}")
        
        except Exception as e:
            logger.error(f"✗ Ошибка обработки сообщения: {e}", exc_info=True)
            
            # Не коммитим при ошибке БД
            # Добавить логику повторных попыток!!!!!!!
    
    def consume(self):
        
        """Основной цикл потребления сообщений"""
        
        logger.info("Запуск CDC consumer...")
        
        try:
            while self.running:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    
                    # Нет сообщений - идем на следующую итерацию
                    
                    continue
                
                if msg.error():
                    logger.error(f"!! Ошибка Kafka: {msg.error()}")
                    
                    # Проверяем на критические ошибки
                    
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        
                        continue
                    continue
                
                # Обрабатываем сообщение
                self.process_message(msg)
                
        except KeyboardInterrupt:
            logger.info("Остановка по сигналу пользователя...")
        
        except Exception as e:
            logger.error(f"!!! Критическая ошибка: {e}", exc_info=True)
        
        finally:
            self.cleanup()
    
    
    def cleanup(self):
        
        """Очистка ресурсов"""
        
        logger.info("Завершение работы...")
        
        if self.consumer:
            
            # Коммитим все оставшиеся оффсеты
            
            self.consumer.commit(asynchronous=False)
            self.consumer.close()
            logger.info(" Kafka consumer закрыт")
        
        if self.db:
            self.db.close()
            logger.info(" Подключение к БД закрыто")


def main():
    consumer = KafkaConsumer()
    
    try:
        consumer.initialize()
        consumer.consume()
    except Exception as e:
        logger.error(f" :( Не удалось запустить consumer: {e}", exc_info=True)
        exit(1)


if __name__ == '__main__':
    main()
