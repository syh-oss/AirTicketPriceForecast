class Car(object):
    def __init__(self, brand, model, color):
        self.brand = brand
        self.model = model
        self.color = color
    def run(self):
        print('i can run')
class GasolineCar(Car):
    def run(self):
        print('i can run with gasoline')

class ElectricCar(Car):
    def __init__(self, brand, model, color, battery):
        super().__init__(brand, model, color)
        # 电池属性
        self.battery = battery

    def run(self):
        print(f'i can run with electric，i has a {self.battery} + "kwh battery"')


print(ElectricCar.__mro__)
print(ElectricCar.mro())