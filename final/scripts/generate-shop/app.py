import json
import os
import random
import signal
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from typing import Dict, List, Any
from pathlib import Path


class ProductGenerator:
    def __init__(self):
        self.categories = [
            "Электроника", "Одежда", "Книги", "Продукты",
            "Игрушки", "Красота", "Спорт", "Автотовары"
        ]

        self.brands = [
            "XYZ", "ABC", "TechPro", "SmartGear", "FutureTech",
            "EcoLife", "HomeStyle", "ProFit", "LuxuryGoods"
        ]

        self.words = [
            "умные", "часы", "смартфон", "ноутбук", "телевизор",
            "наушники", "монитор", "клавиатура", "мышь", "планшет",
            "фотоаппарат", "колонка", "роутер", "принтер", "сканер"
        ]

        self.descriptions = [
            "Высокотехнологичное устройство с передовыми функциями.",
            "Идеальное сочетание стиля и функциональности.",
            "Инновационный продукт для современного образа жизни.",
            "Надежное и качественное изделие от проверенного производителя.",
            "Стильный дизайн и премиальное качество сборки."
        ]

        self.tags_options = [
            ["умные", "часы", "гаджеты", "технологии", "бан"],
            ["электроника", "техника", "смарт", "девайсы"],
            ["мода", "стиль", "аксессуары", "тренды", "ченсоред"],
            ["дом", "быт", "уют", "интерьер"],
            ["спорт", "здоровье", "фитнес", "активный", "контрафакт", "фууу"]
        ]

        self.currencies = ["RUB", "USD", "EUR"]

        self.specs = {
            "weight": ["50g", "100g", "150g", "200g", "250g"],
            "dimensions": ["42mm x 36mm x 10mm", "50mm x 40mm x 12mm", "60mm x 45mm x 15mm"],
            "battery_life": ["24 hours", "36 hours", "48 hours", "72 hours"],
            "water_resistance": ["IP68", "IP67", "IPX7", "IPX5"]
        }

        self.image_urls = [
            "https://example.com/images/product{num}.jpg",
            "https://example.com/images/product{num}_front.jpg",
            "https://example.com/images/product{num}_side.jpg",
            "https://example.com/images/product{num}_back.jpg",
            "https://example.com/images/product{num}_detail.jpg"
        ]

        self.running = True

        # Загрузка имени файла из переменной окружения
        self.default_output_file = os.getenv("PRODUCTS_OUTPUT_FILE", "products.json")

    def _convert_to_milliseconds(self, iso_timestamp: str) -> int:
        """Преобразует строку в формате ISO8601 в миллисекунды."""
        dt = datetime.fromisoformat(iso_timestamp.rstrip("Z"))
        return int(dt.timestamp() * 1000)

    def generate_product(self, index: int) -> Dict[str, Any]:
        """Генерация одного продукта"""
        product_id = str(random.randint(10000, 99999))
        category = random.choice(self.categories)
        brand = random.choice(self.brands)

        # Генерация имени
        name_words = random.sample(self.words, random.randint(2, 4))
        name = f"{brand} {' '.join(name_words).title()}"

        # Генерация описания
        description = random.choice(self.descriptions)

        # Генерация цены
        price_amount = round(random.uniform(99.99, 9999.99), 2)
        price_currency = random.choice(self.currencies)

        # Генерация остатков
        available = random.randint(0, 500)
        reserved = random.randint(0, min(available, 50))

        # Генерация SKU
        sku = f"{brand[:3]}-{random.randint(10000, 99999)}"

        # Генерация тегов
        base_tags = random.choice(self.tags_options)
        extra_tags = random.sample([w for w in self.words if w not in base_tags],
                                  random.randint(1, 3))
        tags = base_tags + extra_tags

        # Генерация изображений
        num_images = random.randint(1, 5)
        images = []
        for i in range(num_images):
            images.append({
                "url": random.choice(self.image_urls).format(num=random.randint(1, 1000)),
                "alt": f"{name} - {['вид спереди', 'вид сбоку', 'вид сзади', 'детальный вид', 'в упаковке'][i % 5]}"
            })

        # Генерация спецификаций
        specifications = {
            "weight": random.choice(self.specs["weight"]),
            "dimensions": random.choice(self.specs["dimensions"]),
            "battery_life": random.choice(self.specs["battery_life"]),
            "water_resistance": random.choice(self.specs["water_resistance"])
        }

        # Генерация дат
        created_days_ago = random.randint(1, 365)
        updated_days_ago = random.randint(0, created_days_ago)

        created_at = (datetime.now() - timedelta(days=created_days_ago)).isoformat() + "Z"
        updated_at = (datetime.now() - timedelta(days=updated_days_ago)).isoformat() + "Z"

        created_at_ms = self._convert_to_milliseconds(created_at)
        updated_at_ms = self._convert_to_milliseconds(updated_at)

        # Генерация store_id
        store_id = f"store_{random.randint(1, 100):03d}"

        return {
            "product_id": product_id,
            "name": name,
            "description": description,
            "price": {
                "amount": price_amount,
                "currency": price_currency
            },
            "category": category,
            "brand": brand,
            "stock": {
                "available": available,
                "reserved": reserved
            },
            "sku": sku,
            "tags": tags,
            "images": images,
            "specifications": specifications,
            "created_at": created_at_ms,  # Используем миллисекунды
            "updated_at": updated_at_ms,  # Используем миллисекунды
            "index": "products",
            "store_id": store_id
        }

    def generate_products(self, num_products: int) -> List[Dict[str, Any]]:
        """Генерация списка продуктов"""
        return [self.generate_product(i) for i in range(num_products)]

    def save_to_file(self, products: List[Dict[str, Any]], filename: str = None):
        """Сохранение продуктов в JSON файл"""
        # Используем переданное имя файла или значение из переменной окружения
        output_filename = filename or self.default_output_file

        with open(output_filename, 'w', encoding='utf-8') as f:
            # Используем параметр separators для удаления лишних пробелов и переносов строк
            json.dump(products, f, ensure_ascii=False, separators=(',', ':'))
        print(f"\n✅ Успешно сохранено {len(products)} записей в файл {output_filename}")

def main():
    """Основная функция"""
    generator = ProductGenerator()
    print("\nВыберите режим работы:")
    print("1. Интерактивный режим (остановка по Ctrl+C)")
    print("2. Сгенерировать определенное количество записей")
    
    choice = input("\nВведите номер режима (1 или 2): ").strip()
    
    if choice == "1":
        generator.interactive_mode()
    elif choice == "2":
        try:
            num_products = int(input("\nВведите количество записей для генерации: "))
            if num_products <= 0:
                print("Количество записей должно быть положительным числом.")
                return
            
            products = generator.generate_products(num_products)
            
            # Генерация имени файла с количеством записей и timestamp
            load_dotenv(dotenv_path=".env")
            base_name = os.getenv("PRODUCTS_OUTPUT_FILE") # , "../data/products")
            SCRIPT_DIR = Path(__file__).parent.absolute()
            print(f"Директория скрипта: {SCRIPT_DIR}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = str(SCRIPT_DIR / f"{base_name}_{num_products}_{timestamp}.json")

            generator.save_to_file(products, filename)
            
        except ValueError:
            print("Пожалуйста, введите корректное число.")
    else:
        print("Неверный выбор. Завершение работы.")

if __name__ == "__main__":
    main()        