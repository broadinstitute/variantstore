F1=204126160095_R01C01.vcf
F2=aou_gda_1.vcf

cat $F1 | grep -v "#" | sort > /tmp/o1.txt
cat $F2 | grep -v "#" | sort > /tmp/o2.txt
python compare_array_vcfs.py /tmp/o1.txt /tmp/o2.txt