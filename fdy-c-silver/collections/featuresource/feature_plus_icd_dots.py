#https://github.com/StefanoTrv/simple_icd_10/blob/master/simple_icd_10.py
def add_dot_to_code(code):
    if len(code)<4 or code[3]==".":
        return code
    elif code[:3]+"."+code[3:] in code_to_node:
        return code[:3]+"."+code[3:]
    else:
        return code
