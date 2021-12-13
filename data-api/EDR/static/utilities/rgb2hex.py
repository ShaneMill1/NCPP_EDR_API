import argparse
from colormap import rgb2hex

def convert(input_file):
   f = open(input_file, "r")
   new_content=list()
   content=f.readlines()
   for c in content:
         rgb_list_filter=list()
         new=c.replace("\t",' ')
         new=new.replace("\n",' ')
         rgb_list=new.split(' ')
         for r in rgb_list:
            if len(r)>0:
               rgb_list_filter.append(r)
         try:
            new=rgb2hex(int(rgb_list_filter[0]),int(rgb_list_filter[1]),int(rgb_list_filter[2]))
         except:
            print(c + ' skipped')
         new_content.append(new)
   f.close()
   return new_content


def write_to_file(new_content,output_file):
   textfile=open('./hex/'+output_file,'w')
   for element in new_content:
      textfile.write(element+"\n")
   textfile.close()
   return


if __name__ == "__main__":
   parser = argparse.ArgumentParser(description='Convert rgb to hex')
   parser.add_argument('file', type = str, help = 'input file')
   args=parser.parse_args()
   input_file=args.file
   output_file=input_file+'.hexpal'
   new_content=convert(input_file)
   write_to_file(new_content,output_file)
   



