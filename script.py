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


def aggregation_store_product(chunk_mothn_transaction, dict_shop):
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
        chunk_mothn_transaction: a file, chunk from the original psv file
        dict_shop: a dictionary

    Returns:
        Dictionary listing each of the shops as a key
    """
    for transaction in chunk_mothn_transaction:
        # one line of a receipt
        _, _,identifiant_produit,code_magasin,_,prix = transaction.split('|')

        # addition of shop and product in dictionary
        if code_magasin in dict_shop.keys():
            if identifiant_produit in dict_shop[code_magasin].keys():
                dict_shop[code_magasin][identifiant_produit] += 1
            else:
                dict_shop[code_magasin][identifiant_produit] = 1

        else:
            dict_shop[code_magasin] = {}
            dict_shop[code_magasin][identifiant_produit] = 1
            dict_shop[code_magasin]['CA'] = float(prix)

        dict_shop[code_magasin]['CA'] += float(prix)
    return dict_shop


def save_top_50_stores(output_dict, output_filename="top-50-store"):
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


def save_top_100_products(month_stores_data):
    
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
        month_stores_data : a dictionary to be used to create a csv file
        
    Returns:
       one file for each key (code_magasin) of the dictionary.
       
    """
    
    col_name = "code_magasin|identifiant_produit|CA"
    
    if not os.path.exists('top-products-by_store'):
        os.makedirs('top-products-by_store')


    for key in month_stores_data:
        with open("top-products-by_store/top-100-products-store-" + str(key) + ".csv", 'w') as csvFile:
            csvFile.write(" ".join(col_name) + "\n")
            
            CA_value = month_stores_data[key]['CA']
            month_stores_data[key].pop('CA',None)
            
            for j, product in enumerate(month_stores_data[key]):
                if j < 100:
                    csvFile.write(" ".join(str(key) + "|" + str(product) + "|" + str(CA_value) + "\n"))



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
            d_product = aggregation_store_product(data, d_product)
            i += 1

    ### sorting different dictionaries
    print("Sorting data")
    top_50_shops = dict(sorted(d_product.items(), key=lambda item: item[1]['CA'], reverse=True)[:50])
    products_order_by_shop = dict(sorted(d_product.items(), key=my_key_order_product, reverse=True))

    ### saving data
    print("Saving data to csv")
    save_top_50_stores(top_50_shops)
    save_top_100_products(products_order_by_shop)


