#my_pipeline.py
def A() -> int:
    '''Constant value 4'''
    return 4

def B(A :int) -> float:
    '''Divide A by 2'''
    return A / 2

def C(A :int) -> int:
    '''Multiply by 5'''
    return A * 5

def D(B :float, C :int) -> float:
    '''Subtract B from C'''
    return C-B

def E(B :float) -> float:
    '''Divide B by 3'''
    return E/3


