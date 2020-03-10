import configparser

def get_param(file, section):
    params = {}
    #initiate ConfigParser constructor
    parser = configparser.ConfigParser()
    parser.read(file)
    if(parser.has_section(section)):
        items = parser.items(section)
        for item in items:
            params[item[0]] = item[1]
    else:
        print("There is no such section '{0}' in {1} file".format(section,file))
    return params

