class MyToy: 
    __slots__ = ['toyId', 'arrivalTime', 'duration']
    def __init__(self, toyId, arrivalTime, duration):
        self.toyId = toyId
        self.arrivalTime = arrivalTime
        self.duration = duration
