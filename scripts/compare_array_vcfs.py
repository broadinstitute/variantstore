import sys

def get_next_line(i):
    for line in i:
        if (line.startswith("#")):
            pass;
        elif ("ZEROED_OUT_ASSAY" in line):
            pass;
        else:
            return line;
     
def parseline(e):
    data = {}
    parts = e.strip().split("\t")

    data['chrom'] = parts[0]
    data['pos'] = parts[1]
    data['id'] = parts[2]
    data['ref'] = parts[3]
    data['alt'] = parts[4]
    data['filter'] = parts[6]

    format_keys = parts[8].split(":")
    call = parts[9]
    format_values = call.split(":")
    format_dict = dict(zip(format_keys, format_values))

    data['gt'] = format_dict['GT']
    data['baf'] = format_dict['BAF']
    data['lrr'] = format_dict['LRR']
    data['normx'] = format_dict['NORMX']
    data['normy'] = format_dict['NORMY']
    return data;

def compare(e1, e2, key, ):
    if (e1[key] != e2[key]):
        print(f"DIFF on {key}")
        print(f"{e1}")
        print(f"{e2}")

def compare_float(e1, e2, key, tolerance):
    # compare directly first, also handles '.' case
    s1 = e1[key]
    s2 = e2[key]
    
    if (s1 != s2):
        if ("." in s1 or "." in s2):
            print(f"DIFF on {key} with values of {e1} and {e2}")
            
        else:
            v1 = float(s1)
            v2 = float(s2)
    
            delta = abs(v2 - v1)
            if delta > tolerance:
                print(f"DIFF on {key} of {delta}")
                print(f"{e1}")
                print(f"{e2}")
        
    
EXACT_FIELDS = ['chrom','pos','id','ref','alt','gt']

#
# NOTE: files should have been passed through "unix sort" first
# i.e. cat foo.vcf | sort > new.vcf
vcf_file_1 = sys.argv[1]
vcf_file_2 = sys.argv[2]

with open(vcf_file_1) as file1, open(vcf_file_2) as file2:
        
    while True:
        line1 = get_next_line(file1)
        line2 = get_next_line(file2)

        if (line1 == None and line2 == None):
            break
            
        e1 = parseline(line1)
        e2 = parseline(line2)
     
        # do the comparison of exact matches
        for key in EXACT_FIELDS:
            compare(e1, e2, key)

        compare_float(e1, e2, 'baf', 0.001)
        compare_float(e1, e2, 'lrr', 0.001)
        compare_float(e1, e2, 'normx', 0.001)
        compare_float(e1, e2, 'normy', 0.001)

