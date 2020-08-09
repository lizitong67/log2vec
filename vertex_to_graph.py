#!/usr/bin/env python

import dgl
import numpy as np
import networkx as nx
import math
import csv
import matplotlib.pyplot as plt


def store_nodes_of_the_same_day():
    print("Function 'store_nodes_of_the_same_day()' starts!")
    vertex_list = []

    # Get nodes from authentication log entries
    with open('data/demo_data/U748@DOM1_auth.csv', 'r', encoding='utf-8') as auth_file:
        read = csv.reader(auth_file)
        for i in read:
            vertex_number = int(i[0])
            timestamp = int(i[6])
            vertex = { 'vertex_type': 'auth',
                       'vertex_number': vertex_number,
                       'sub': i[1],
                       'obj': i[2],
                       'A': i[3],
                       'auth_type': i[4],
                       'logon_type': i[5],
                       'T': timestamp,
                       'H': i[7]
                      }
            vertex_list.append(vertex)

    # Get nodes from process log entries
    with open('data/demo_data/U748@DOM1_proc.csv', 'r', encoding='utf-8') as proc_file:
        read = csv.reader(proc_file)
        for i in read:
            vertex_number = int(i[0])
            timestamp = int(i[4])
            vertex = { 'vertex_type': 'proc',
                       'vertex_number': vertex_number,
                       'sub': i[1],
                       'obj': i[2],
                       'A': i[3],
                       'T': timestamp,
                       'H': i[5]
                      }
            vertex_list.append(vertex)

    # Sort the vertex in chronological order
    sorted_vertex_list = sorted(vertex_list, key=lambda e: e.__getitem__('T'))

    # daily sequence graph list
    daily_sequences_list = [None] * 58

    for i in sorted_vertex_list:
        vertex = i
        timestamp = vertex['T']
        # Day of the vertex, and actual day should be increased by 1
        day_of_vertex = timestamp // 86400

        # If the sequence graph not exists, create it
        if not daily_sequences_list[day_of_vertex]:
            daily_sequences_list[day_of_vertex] = nx.MultiGraph()

        if vertex['vertex_type'] == 'auth':
            daily_sequences_list[day_of_vertex].add_node(vertex['vertex_number'], type=vertex['vertex_type'],
                                                         sub=vertex['sub'], obj=vertex['obj'], A=vertex['A'],
                                                         AuthType=vertex['auth_type'],
                                                         LogonType=vertex['logon_type'],
                                                         T=vertex['T'], H=vertex['H'])
        if vertex['vertex_type'] == 'proc':
            daily_sequences_list[day_of_vertex].add_node(vertex['vertex_number'], type=vertex['vertex_type'],
                                                         sub=vertex['sub'], obj=vertex['obj'], A=vertex['A'],
                                                         T=vertex['T'], H=vertex['H'])

    print("All nodes are stored in the sequence graphs according to the day!")

    return daily_sequences_list

def rule_1(daily_sequences_list):
    print ("Function 'rule_1()' starts!")
    # Associate nodes according to Rule 1
    for daily_sequence in daily_sequences_list:
        if daily_sequence:
            # Transform the reportviews.NodeView data type to list
            node_list = list(daily_sequence.nodes())
            for i in range(1, len(node_list)):
                day_of_seq = daily_sequences_list.index(daily_sequence)
                current_seq = daily_sequences_list[day_of_seq]
                current_seq.add_edge(node_list[i-1], node_list[i], EdgeType=1, weight=1)
    print ("All nodes in sequence graphs are associated into sequences!")
    return daily_sequences_list

def rule_23(daily_sequences_list):
    print ("Function 'rule_23()' starts!")

    # Associate nodes according to Rule 2 and Rule 3
    # list of 58 daily sequences >> tuple{H:[node numbers]}
    H_tuple_list = [None]*58
    # list of 58 daily sequences >> tuple{H:tuple} >> tuple{A:[node numbers]}
    A_tuple_list = [None]*58
    for daily_sequence in daily_sequences_list:
        if daily_sequence:
            # key: H;    value: list of nodes number
            H_record_tuple = {}
            node_list = list(daily_sequence.nodes())
            for node_i in node_list:
                current_H = daily_sequence.nodes[node_i]['H']
                if current_H not in H_record_tuple.keys():
                    H_record_tuple[current_H] = [node_i]
                else:
                    node_j = H_record_tuple[current_H][-1]
                    daily_sequence.add_edge(node_j, node_i, EdgeType=2, weight=1)
                    H_record_tuple[current_H].append(node_i)

            A_record_tuple_tuple = {}
            # key represents H
            for key in H_record_tuple:
                # Nodes in H_list have the same H
                H_list = H_record_tuple[key]
                A_record_tuple = {}
                for node_i in H_list:
                    current_A = daily_sequence.nodes[node_i]['A']
                    if current_A not in A_record_tuple.keys():
                        A_record_tuple[current_A] = [node_i]
                    else:
                        node_j = A_record_tuple[current_A][-1]
                        daily_sequence.add_edge(node_j, node_i, EdgeType=3, weight=1)
                        A_record_tuple[current_A].append(node_i)

                A_record_tuple_tuple[key]=A_record_tuple

            day_of_seq = daily_sequences_list.index(daily_sequence)
            H_tuple_list[day_of_seq] = H_record_tuple
            A_tuple_list[day_of_seq] = A_record_tuple_tuple


    print ("Edges are added based on rule2 and rule3 in daily sequence!")
    return daily_sequences_list, H_tuple_list, A_tuple_list

def rule_456(daily_sequences_list, H_tuple_list, A_tuple_list):
    print ("Function 'rule_456()' starts!")

    # Associate daily sequences according to Rule 4
    graph = nx.MultiGraph()

    # Add all daily sequences to the graph
    for daily_sequence in daily_sequences_list:
        if daily_sequence:
            graph = nx.compose(graph,daily_sequence)

    # Add edges between daily sequences
    for i in range(0, 58):
        for j in range(i+1, 58):
            if daily_sequences_list[i] and daily_sequences_list[j]:
                node_list_i = list(daily_sequences_list[i].nodes())
                node_list_j = list(daily_sequences_list[j].nodes())
                u1 = node_list_i[0]
                v1 = node_list_j[0]
                u2 = node_list_i[-1]
                v2 = node_list_j[-1]
                len_u = len(node_list_i)
                len_v = len(node_list_j)
                weight_u_v = len_u / len_v if len_u < len_v else len_v / len_u
                w = round(weight_u_v,3)
                graph.add_edge(u1, v1, EdgeType=4, weight=w)
                graph.add_edge(u2, v2, EdgeType=4, weight=w)

                # Add edges based on Rule 5 and Rule 6
                # key represents H
                for key in H_tuple_list[i]:
                    if key in H_tuple_list[j].keys():
                        u1 = H_tuple_list[i][key][0]
                        v1 = H_tuple_list[j][key][0]
                        u2 = H_tuple_list[i][key][-1]
                        v2 = H_tuple_list[j][key][-1]
                        len_u = len(H_tuple_list[i][key])
                        len_v = len(H_tuple_list[j][key])
                        weight_u_v = len_u / len_v if len_u < len_v else len_v / len_u
                        w = round(weight_u_v, 3)
                        graph.add_edge(u1, v1, EdgeType=5, weight=w)
                        graph.add_edge(u2, v2, EdgeType=5, weight=w)

                        for operation_type in A_tuple_list[i][key]:
                            if operation_type in A_tuple_list[j][key]:
                                u1 = A_tuple_list[i][key][operation_type][0]
                                v1 = A_tuple_list[j][key][operation_type][0]
                                u2 = A_tuple_list[i][key][operation_type][-1]
                                v2 = A_tuple_list[j][key][operation_type][-1]
                                len_u = len(A_tuple_list[i][key][operation_type])
                                len_v = len(A_tuple_list[j][key][operation_type])
                                weight_u_v = len_u / len_v if len_u < len_v else len_v / len_u
                                w = round(weight_u_v, 3)
                                graph.add_edge(u1, v1, EdgeType=6, weight=w)
                                graph.add_edge(u2, v2, EdgeType=6, weight=w)

    print ("Edges are added based on rule 7 and rule 8 in graph!")
    return graph

def rule78(graph):
    print ("Function 'rule_78()' starts!")
    node_list = list(graph.nodes())
    src_des_autype_tuple={}
    for node_i in node_list:
        if graph.nodes[node_i]['type']=='auth' and graph.nodes[node_i]['AuthType'] != '?':
            src_des_autype = graph.nodes[node_i]['H']+'&'+graph.nodes[node_i]['obj']+'&'+graph.nodes[node_i]['AuthType']
            if src_des_autype not in src_des_autype_tuple.keys():
                src_des_autype_tuple[src_des_autype]=[node_i]
            else:
                node_j = src_des_autype_tuple[src_des_autype][-1]
                graph.add_edge(node_j, node_i, EdgeType=7, weight=1)
                src_des_autype_tuple[src_des_autype].append(node_i)

    # The key_list records the key of src_des_autype_tuple, the format type of key is src&des&autype
    key_list = []
    for key in src_des_autype_tuple:
        key_list.append(key)
    for i in range(0,len(key_list)):
        src_des_i = key_list[i].split('&')[0] + '&' + key_list[i].split('&')[1]
        auth_type_i = key_list[i].split('&')[2]
        for j in range(i+1, len(key_list)):
            src_des_j = key_list[j].split('&')[0] + '&' + key_list[j].split('&')[1]
            auth_type_j = key_list[j].split('&')[2]
            u1 = src_des_autype_tuple[key_list[i]][0]
            v1 = src_des_autype_tuple[key_list[j]][0]
            u2 = src_des_autype_tuple[key_list[i]][-1]
            v2 = src_des_autype_tuple[key_list[j]][-1]
            len_u = len(src_des_autype_tuple[key_list[i]])
            len_v = len(src_des_autype_tuple[key_list[j]])
            weight_u_v = len_u / len_v if len_u < len_v else len_v / len_u
            w = round(weight_u_v, 3)
            if src_des_i == src_des_j:
                graph.add_edge(u1, v1, EdgeType=8.1, weight=w)
                graph.add_edge(u2, v2, EdgeType=8.1, weight=w)
            if auth_type_i == auth_type_j:
                graph.add_edge(u1, v1, EdgeType=8.2, weight=w)
                graph.add_edge(u2, v2, EdgeType=8.2, weight=w)
    return graph

if __name__ ==  '__main__':
    print('-----------------------------------------------------------------------')
    daily_sequences_list = store_nodes_of_the_same_day()
    print('-----------------------------------------------------------------------')
    daily_sequences_list = rule_1(daily_sequences_list)
    print('-----------------------------------------------------------------------')
    daily_sequences_list, H_tuple_list, A_tuple_list = rule_23(daily_sequences_list)
    print('-----------------------------------------------------------------------')
    graph = rule_456(daily_sequences_list, H_tuple_list, A_tuple_list)
    print('-----------------------------------------------------------------------')
    graph = rule78(graph)
    print('-----------------------------------------------------------------------')
    print('number of nodes', graph.number_of_nodes())
    print('number of edges', graph.number_of_edges())
    # with open('nodes.txt', 'a+', encoding='utf-8') as file:
    #     file.write(str(graph.nodes.data()))
    # with open('edges.txt', 'a+', encoding='utf-8') as file:
    #     file.write(str(graph.edges.data()))
    print('-----------------------------------------------------------------------')
    nx.write_gml(graph, "data/demo_data/graph.gml")
    print("The graph is stored locally!")
    # graph = nx.read_gml("data/demo_data/graph.gml")
    print('-----------------------------------------------------------------------')

















