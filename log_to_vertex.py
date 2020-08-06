import csv


# Transform the raw data of process into graph vertex
def proc_to_vertex():
    vertex_number = 0
    with open('data/raw_data/proc.txt', 'r', encoding='utf-8') as file:
        for line in file:

            """
            original features of proc log entry:             
            time, user@domain, computer, process name, start/end
            """
            features = line.split(',')

            """
            Adjust to vertex format: <vertex_number, sub(user@domain), obj(process_name), A(start/end), T, H>
            """
            vertex = []
            vertex.append(vertex_number)    # vertex_number
            vertex.append(features[1])  # sub
            vertex.append(features[3])  # obj
            vertex.append(features[4])  # A
            vertex.append(features[0])  # T
            vertex.append(features[2])  # H

            vertex_number += 1

            # Split data according to the sub
            file_path = 'data/proc/' + vertex[1] + '.csv'

            csv_file = open(file_path, 'a+')
            writer = csv.writer(csv_file)
            writer.writerow(vertex)

def auth_to_vertex():
    vertex_number = 0
    for i in range (1, 9):
        raw_data_file = 'data/raw_data/split_auth_file/auth_' + i + '.txt'
        print ("Processing file " + str(i) + '!')
        with open(raw_data_file, 'r', encoding='utf-8') as file:
            for line in file:

                """
                Original features of auth log entry:             
                time, source user@domain, destination user@domain, source computer, destination computer,             
                authentication type, logon type, authentication orientation, success/failure”
                """
                features = line.split(',')

                """
                Adjust to vertex format: <vertex_number, sub(source user@domain), obj(destination computer),
                                         A(authentication orientation), T, H(source computer)>
                """

                vertex = []
                vertex.append(vertex_number)    # vertex_number
                vertex.append(features[1])  # sub
                vertex.append(features[4])  # obj: regard destination computer as obj
                vertex.append(features[7])  # A
                vertex.append(features[5])  # auth type belongs to A
                vertex.append(features[6])  # logon type belongs to A
                vertex.append(features[0])  # T
                vertex.append(features[3])  # H: regard source computer as H

                vertex_number += 1

                if '@?' in vertex[1]:
                    tmp_list = list(vertex[1])
                    tmp_list[-1:] = 'Unknown'
                    vertex[1] = "".join(tmp_list)

                # Split data according to the sub
                file_path = 'data/auth/' + vertex[1] + '.csv'

                csv_file = open(file_path, 'a+')
                writer = csv.writer(csv_file)
                writer.writerow(vertex)

        print("File " + str(i) ' process done!' + '\n')


# proc_to_vertex()
# auth_to_vertex()







