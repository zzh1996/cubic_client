def size(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024:
            return "%.1f %s%s" % (num, unit, suffix)
        num /= 1024
    return "%.1f %s%s" % (num, 'Yi', suffix)
