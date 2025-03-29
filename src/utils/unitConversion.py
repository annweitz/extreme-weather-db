# src/utils/unit_conversion.py

unitConversions = {
    "Kelvin": {
        "Fahrenheit": lambda x: ((x - 273.15) * 1.8) + 32,
        "Celsius": lambda x: x - 273.15
    },
    "m water equivalent": {
        "mm water equivalent": lambda x: x * 1000,
        "cm water equivalent": lambda x: x * 100
    },
    "meter per second": {
        "miles per hour": lambda x: x * 2.236937,
        "kilometer per hour": lambda x: x * 3.6,
        "knots": lambda x: x * 1.9438445
    }
}


def getAvailableConversions(fromUnit):
    return list(unitConversions[fromUnit].keys())


def getConversionFunction(fromUnit, toUnit):
    return unitConversions[fromUnit][toUnit]

