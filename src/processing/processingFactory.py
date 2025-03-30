class ProcessingFactory:

    processingFunctions = {}

    @classmethod
    def registerProcessor(cls, variableName, function):
        cls.processingFunctions[variableName] = function

    @classmethod
    def getProcessor(cls, variableName):
        function = cls.processingFunctions.get(variableName)
        if not function:
            raise ValueError(f"No processor found for variable {variableName}")
        return function

# Register processors

from processPrecipitation import processPrecipitation
from processWindgust import processWindgust
from processTemperature import processTemperature
from processWind import processWind

ProcessingFactory.registerProcessor("precipitation", lambda path: processPrecipitation(path))
ProcessingFactory.registerProcessor("temperature", lambda path: processTemperature(path))
ProcessingFactory.registerProcessor("wind", lambda path: processWind(path))
ProcessingFactory.registerProcessor("windgust", lambda path: processWindgust(path))