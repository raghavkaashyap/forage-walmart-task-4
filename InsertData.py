import csv
import sqlite3

con = sqlite3.connect('shipment_database.db')
cursor = con.cursor()
cursor.execute('Create table Spreadsheet0 (origin_warehouse, destination, product, on_time, product_quantity, driver_identifier)')
with open('data/shipping_data_0.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
    for row in reader:
        cursor.execute('insert into Spreadsheet0 values(?, ?, ?, ?, ?, ?)', row)

cursor.execute('Create table if not exists Spreadsheet12(product, quantity, origin, destination, on_time, driver_identifier)')
data1 = {}
with open('data/shipping_data_1.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        shipment_id = row['shipment_identifier']
        product = row['product']
        on_time = row['on_time']
        if shipment_id not in data1:
            data1[shipment_id] = {}
        if product not in data1[shipment_id]:
            data1[shipment_id][product] = {'quantity': 0, 'on_time': on_time}
        data1[shipment_id][product]['quantity'] += 1

data2 = {}
with open('data/shipping_data_2.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        ship_id = row['shipment_identifier']
        data2[ship_id] = {'origin_warehouse': row['origin_warehouse'], 'destination': row['destination_store'], 'driver_identifier': row['driver_identifier']}

for shipment_id, products in data1.items():
    origin = data2[shipment_id]['origin_warehouse']
    destination = data2[shipment_id]['destination']
    driver_identifier = data2[shipment_id]['driver_identifier']
    for product, quantity in products.items():
        cursor.execute('Insert into Spreadsheet12 values(?,?,?,?,?,?)', (product, quantity['quantity'], origin, destination, on_time, driver_identifier))

con.commit()
con.close()



