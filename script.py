import os

PSV_FILE = 'randomized-transactions-202009.psv'


def read_file_chunks(file_object, chunk_size=1000000):
    """
    Generator to read a file by chunks

    Args:
        file_object: a file object
        chunk_size: size of chunk in bytes

    Returns:
        the lines through which the given chunk_size spans, starting from the current line.
    """

    while True:
        chunked_data = file_object.readlines(chunk_size)
        if not chunked_data:
            break
        yield chunked_data


def dict_store_product(chunk_file, dict_d):
    """
    Takes in a file and a dictionary containing 'CA' and 'identifiant_produit' as keys
    and generates a nested dictionary where each key represents a shop.
    The dictionary has the following structure:
        {
            "code_magasin_1" : {'CA': revenue ,
                                'identifiant_produit_1' : number of occurence in the store,
                                'identifiant_produit_2' : number of occurence in the store ....},
            "code_magasin_2" : {'CA': revenue ,
                                'identifiant_produit_1' : number of occurence in the store,
                                'identifiant_produit_2' : number of occurence in the store ....},
             ....
        }
    Args:
        chunk_file: a file, chunk from the original psv file
        dict_d: a dictionary

    Returns:
        Dictionary listing each of the shops as a key
    """

    for line in chunk_file:
        # one line of a receipt
        line_data = line.split('|')

        # addition of shop and product in dictionary
        if line_data[3] in dict_d.keys():
            if line_data[2] in dict_d[line_data[3]].keys():
                dict_d[line_data[3]][line_data[2]] += 1
            else:
                dict_d[line_data[3]][line_data[2]] = 1

        else:
            dict_d[line_data[3]] = {}
            dict_d[line_data[3]][line_data[2]] = 1
            dict_d[line_data[3]]['CA'] = 0

        dict_d[line_data[3]]['CA'] += float(line_data[5])
    return dict_d


def save_data_dict_store(output_dict, output_filename="top-50-store"):
    """
    Saves the dictionary as a csv file.
    The dictionary is supposed to have this structure :
        {
            "code_magasin_1" : {'CA': revenue ,
                                'identifiant_produit_1' : number of occurence in the store,
                                'identifiant_produit_2' : number of occurence in the store ....},
            "code_magasin_2" : {'CA': revenue ,
                                'identifiant_produit_1' : number of occurence in the store,
                                'identifiant_produit_2' : number of occurence in the store ....},
             ....
        }
    Args:
        output_dict: a dictionary to be used to create a csv file
        output_file: name of the output file 

    """
    col_name = "code_magasin|CA"
    output_file = output_filename + ".csv"
    
    with open(output_file, 'w') as csvFile:
        csvFile.write(" ".join(col_name) + "\n")
        for key in output_dict:
            csvFile.write(" ".join(str(key) + "|" + str(output_dict[key]['CA']) + "\n"))


def save_data_dict_product(output_dict):
    """
    Saves the dictionary as a csv file. 
    the dictionary is supposed to have this structure :
        {
            "code_magasin_1" : {'CA': revenue ,
                                'identifiant_produit_1' : number of occurence in the store,
                                'identifiant_produit_2' : number of occurence in the store ....},
            "code_magasin_2" : {'CA': revenue ,
                                'identifiant_produit_1' : number of occurence in the store,
                                'identifiant_produit_2' : number of occurence in the store ....},
             ....
        }
    Args:
        output_dict: a dictionary to be used to create a csv file
        
    Returns:
       one file for each key (code_magasin) of the dictionary.
       
    """
    col_name = "code_magasin|identifiant_produit|CA"
    try:
        os.makedirs('top-products-by_store')
    except OSError:
        pass
    for key in output_dict:
        with open("top-products-by_store/top-100-products-store-" + str(key) + ".csv", 'w') as csvFile:
            csvFile.write(" ".join(col_name) + "\n")
            j = 0
            for i in output_dict[key]:
                if i != 'CA' and j < 100:
                    csvFile.write(" ".join(str(key) + "|" + str(i) + "|" + str(output_dict[key]['CA']) + "\n"))
                    j += 1


def my_key_order_product(input_dict):
    """
    Sorts a dictionary without considering the CA
    
    Args:
        output_dict: a dictionary to sort
        
    Returns:
       one identifiant_produit for each key (code_magasin) of the dictionary.
    
    """
    for i in input_dict[1]:
        if i == 'CA':
            pass
        else:
            return i


if __name__ == "__main__":
    d_product = {}
    i = 0
    print("Processing data")
    with open(PSV_FILE) as f:
        next(f)
        for data in read_file_chunks(f):
            if i % 1000 == 0:
                print('iterations: ',i)
            d_product = dict_store_product(data, d_product)
            i += 1

    ### sorting different dictionaries
    print("Sorting data")
    data_order_50 = dict(sorted(d_product.items(), key=lambda item: item[1]['CA'], reverse=True)[:50])
    product_order = dict(sorted(d_product.items(), key=my_key_order_product, reverse=True))

    ### saving data
    print("Saving data to csv")
    save_data_dict_store(data_order_50)
    save_data_dict_product(product_order)


