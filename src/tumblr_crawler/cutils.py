from ConfigParser import NoOptionError

def get_from_config(config, section, option, default = None):
    
    try:
        value = config.get(section, option)
        try:
            fvalue = float(value)
            ivalue = int(fvalue)
            if ivalue == fvalue:
                return ivalue
            else:
                return fvalue
        except ValueError:
            return value
    except NoOptionError:
        return default