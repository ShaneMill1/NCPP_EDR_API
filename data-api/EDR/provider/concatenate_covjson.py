import argparse
import copy
import json
def concat_covjson(c_list):
   f=open("EDR/provider/schemas/coverage_collection_schema.json")
   #f=open('/home/smill/WIAB-nomads-rep/NOMADS-EDR-rep/modules/schemas/coverage_collection_schema.json')
   schema=json.load(f)
   new_dict=schema
   cov_dict_list=list()
   for idc,cov in enumerate(c_list):
      cov_dict={}
      cov_dict=copy.deepcopy(schema["coverages"][0])
      cov_dict.update({"name":'trajectory_'+str(idc)})
      dic_cov=cov
      new_dict['domainType']=copy.deepcopy(dic_cov['domain']['domainType'])
      new_dict['referencing']=copy.deepcopy(dic_cov['domain']['referencing'])
      for idx,param in enumerate(dic_cov['parameters']):
            #new_dict['parameters'][param]=schema['parameters']['pname']
            new_dict['parameters'][param]=copy.deepcopy(dic_cov['parameters'][param])
            cov_dict['ranges'][param]=copy.deepcopy(cov_dict['ranges']['pname'])
            cov_dict['ranges'][param]['shape']=copy.deepcopy(dic_cov['ranges'][param]['shape'])
            cov_dict['ranges'][param]['axisNames']=copy.deepcopy(dic_cov['ranges'][param]['axisNames'])
            cov_dict['ranges'][param]['values']=copy.deepcopy(dic_cov['ranges'][param]['values'])
      cov_dict['domain']['axes']=copy.deepcopy(dic_cov['domain']['axes'])
      dic_copy=copy.deepcopy(cov_dict)
      cov_dict_list.append(dict(dic_copy))
   del new_dict['parameters']['pname']
   new_dict["coverages"]=cov_dict_list
   for idcc,covc in enumerate(c_list):
      del new_dict['coverages'][idcc]['ranges']['pname']
   #with open(output_file, 'w') as fp:
   #   json.dump(new_dict, fp, indent=2)
   return new_dict
