import importlib as il 

def import_pycallable(pycallable):    
    """
    Takes a string and returns module and method
    Copied from MetOcean's internal ops_core so that
    this can be stand-alone
    """
    pycallable = pycallable.split('.')
    method = pycallable[-1]
    module_str = '.'.join(pycallable[:-1])
    module = il.import_module(module_str)
    return getattr(module, method)
