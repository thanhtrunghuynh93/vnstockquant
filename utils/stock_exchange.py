def is_trading_hour(t):
    import pdb
    pdb.set_trace()
    if t.tm_wday not in [0, 1, 2, 3, 4]:
        return False
    if t.tm_hour not in [9, 10, 11, 13, 14]:
        return False
    if t.tm_min not in [0, 15, 30, 45]:
        return False    
    if (t.tm_hour, t.tm_min) in [(9, 0), (11, 45)]:
        return False

    return True    

