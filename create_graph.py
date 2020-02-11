#!/usr/bin/python
# created by xuying li
# use py2neo and neo4j to create graph

import pandas as pd
from py2neo import *

def main():

	# use neo4j desktop to create the project 
	# then get the url, username and password from it
	
	graph =  Graph("http://localhost:7474",username="neo4j",password="12345")

	# read the csv file
	data = pd.read_csv('all_one_to_one relations.csv')
	data_strings = pd.read_csv('LUI-SUI.csv')
	data_LUI = pd.read_csv('CUI-LUI.csv')

	create_basic(data, graph)
	create_bruch(data_strings, graph)
	create_the_rest(data_LUI, graph)


def create_basic(data, graph):
	# get all the data to list 
	concept_total = data['concept'].values.tolist()
	P = data['P'].values.tolist()
	lexicon = data['lexicon'].values.tolist()
	PF = data['PF'].values.tolist()
	string = data['string'].values.tolist()
	name_total = data['name'].values.tolist()

	for i in range(len(concept_total)):
		node =  Node('SUI', name=name_total[i])
		node['id'] = string[i]
		graph.create(node)
		self_loop = Relationship(node , 'PF' , node) 
		graph.create(self_loop)
		lexicom = Node('LUI', name = name_total[i])
		lexicom['id'] = lexicon[i]
		graph.create(lexicom)

		string_to_lexicom = Relationship(node , 'PF' , lexicom) 
		graph.create(string_to_lexicom)

		concept = Node('CUI', name = name_total[i])
		concept['id'] = concept_total[i]
		concept_to_lexicom = Relationship(lexicom, 'P', concept)
		graph.create(concept)
		graph.create(concept_to_lexicom)


def create_bruch(data_strings, graph):
	data_strings_1 = data_strings[data_strings['relations'] != 'PF']

	matcher = NodeMatcher(graph)
	non_one_LUI = []
	LUI_list = list(set(data_strings_1['LUI'].values.tolist()))
	LUI_result = list(set(lexicon) & set(LUI_list))

	for LUI in LUI_result:
		# print('The current LUI is: ', LUI)
		rows = data_strings[data_strings['LUI'] == LUI]
		row_ori = rows[data_strings['relations'] == 'PF']
		if len(row_ori) != 1:
			print('The current LUI has multiple PF: ', LUI)
			continue

		rows = rows.reset_index()

		s = list(row_ori['SUI'])[0]

		node_origin = graph.evaluate("MATCH (SUI { id:  '"+ s +"' }) RETURN SUI")

		for i in range(len(rows)):
            row = rows.loc[i]

            if row.relations == 'PF':
                print('Current LUI: ',LUI)
                continue
            #......................................#
            if row.relations == 'VO':
                #......................................#
                if len(list(matcher.match('SUI', id = row['SUI']))) == 0:

                	create_node(row, matcher, 'VO', node_origin)

                    continue
                    
                else:
                    print('This SUI already exits (VO): ', row)
                    
            elif row.relations == 'VW':
                
                if len(list(matcher.match('SUI', id = row['SUI']))) == 0:

                	create_node(row, matcher, 'VW', node_origin)

                    continue
                else:
                    print('This SUI already exits (VW): ', row['SUI'])
                    
            
            #......................................#
            elif row.relations == 'VCW':
                #......................................#
                
                if len(list(matcher.match('SUI', id = row['SUI']))) == 0:

                	create_node(row, matcher, 'VCW', node_origin)
                else:
                    print('This SUI already exits (VCW): ', row['SUI'])
                    
            #......................................#
            elif row.relations == 'VC':
                #......................................#
                
                if len(list(matcher.match('SUI', id = row['SUI']))) == 0:
                	create_node(row, matcher, 'VC', node_origin)

                else:
                    print('This SUI already exits: ', row['SUI'])

def create_the_rest(data_LUI, data_strings, graph):
	matcher = NodeMatcher(graph)
	data_LUI = data_LUI[data_LUI['P/S'] == 'S']
	LUI_list = list(set(data_LUI['LUI']))

	for item in LUI_list:
		if len(data_LUI_SUI.loc[(data_LUI_SUI['LUI'] == item) & (data_LUI_SUI['relations'] == 'PF')]) != 1:
			result.remove(item)
			print('multiple perfer: ', item)
			continue

		rows = data_LUI_SUI[data_LUI_SUI['LUI'] == item]
		rows = rows.reset_index()

		# find the P LUI
		CUI = all_data_LUI[all_data_LUI['LUI'] == item]['CUI']
		CUI = CUI.values.tolist()[0]
		s = all_data_LUI[all_data_LUI['CUI'] == CUI]
		s = s[s['P/S'] == 'P']
		s = s['LUI'].values.tolist()
		if len(matcher.match('LUI', id = s[0])) == 0:
			continue

		for i in range(len(rows)):
			row = rows.loc[i]
			if row['relations'] == 'PF':
				if len(list(matcher.match('SUI', id = row['SUI']))) == 0:
					node_ori =  Node('SUI', name=row['string_name'])
					node_ori['id'] = row['SUI']
					graph.create(node_ori)
					self_loop = Relationship(node_ori , 'PF' , node_ori) 
					graph.create(self_loop)

					lexicom = Node('LUI', name = row['string_name'])
					lexicom['id'] = row['LUI']
					graph.create(lexicom)
					print('The LUI node create complete')

					string_to_lexicom = Relationship(node_ori , 'PF' , lexicom) 
					graph.create(string_to_lexicom)
				else:
					continue

				# connect S LUI to P LUI 
				LUI_origin = graph.evaluate("MATCH (LUI { id:  '"+ s[0] +"' }) RETURN LUI")
				LUI_to_origin = Relationship(lexicom, 'P', LUI_origin)
				graph.create(LUI_to_origin)
				origin_to_LUI = Relationship(LUI_origin, 'S', lexicom)
				graph.create(origin_to_LUI)

			if row['relations'] == 'VO':
				if len(list(matcher.match('SUI', id = row['SUI']))) == 0:
					create_node(row, matcher, 'VO', node_ori)
				else:
					continue

			elif row['relations'] == 'VC':
				if len(list(matcher.match('SUI', id = row['SUI']))) == 0:
					create_node(row, matcher, 'VC', node_ori)
				else:
					continue

			elif row['relations'] == 'VW':
				if len(list(matcher.match('SUI', id = row['SUI']))) == 0:
					create_node(row, matcher, 'VW', node_ori)
				else:
					continue

			elif row['relations'] == 'VCW':
				if len(list(matcher.match('SUI', id = row['SUI']))) == 0:
					create_node(row, matcher, 'VCW', node_ori)
				else:
					continue

def create_node(row, matcher, re, node_origin):
	# create a SUI node
	node =  Node('SUI', name= row['string_name'])
	node['id'] = row['SUI']
	graph.create(node)
	print('Node Create finishes: ',row['SUI'])
	#create the relation
	#......................................#
	origin_to_node = Relationship(node_origin, re, node)
	#......................................#
	graph.create(origin_to_node)
	node_to_orign = Relationship(node, 'PF', node_origin)
	graph.create(node_to_orign)
	print('relation create finishes')



if __name__ == '__main__':
	main()