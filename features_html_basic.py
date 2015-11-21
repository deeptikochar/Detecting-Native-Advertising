import boto
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import re
import csv
from csv import DictWriter


bucket = 'bds-new'
def process_data(s3key):
    """
    Fetch data with the given s3 key and pass along the contents as a string.

    :param s3key: An s3 key path string.
    :return: A tuple (file_name, data) where data is the contents of the 
        file in a string. Note that if the file is compressed the string will 
        contain the compressed data which will have to be unzipped using the 
        gzip package.
    """
    print "FETCH_DATA gets called"
    conn = boto.connect_s3()
    b = conn.get_bucket(bucket, validate=False)
    k = b.get_key(s3key)
    data = k.get_contents_as_string()
    values = {}
    # HTML features
    values['filename'] = os.path.basename(s3key)
    values['div_count'] = data.count('<div')
    values['paragraph_count'] = data.count('<p>')
    values['header_count'] = data.count('<h')
    values['a_count'] = data.count('<a>') + data.count('<a ')
    values['link_count'] = data.count('<link')
    values['nav_count'] = data.count('<nav')
    values['table_count'] = data.count('<table')
    values['tr_count'] = data.count('<tr')
    values['td_count'] = data.count('<td')
    values['svg_count'] = data.count('<svg')
    values['quotation_count'] = data.count('<q')
    values['h1_count'] = data.count('<h1')
    values['h2_count'] = data.count('<h2')
    values['h3_count'] = data.count('<h3')
    values['h4_count'] = data.count('<h4')
    values['h5_count'] = data.count('<h5')
    values['h6_count'] = data.count('<h6')
    values['br_count'] = data.count('<br>')
    values['hr_count'] = data.count('<hr')
    values['abbr_count'] = data.count('<abbr')  
    values['address_count'] = data.count('<address')
    values['iframe_count'] = data.count('<iframe')
    values['bold_count'] = data.count('<b>') + data.count('<b ')
    values['audio_count'] = data.count('<audio')
    values['source_count'] = data.count('<source')
    values['video_count'] = data.count('<video')
    values['track_count'] = data.count('<track')
    values['script_count'] = data.count('<script')
    values['noscript_count'] = data.count('<noscript')
    values['embed_count'] = data.count('<embed')
    values['object_count'] = data.count('<object')
    values['param_count'] = data.count('<param')  
    values['bdi_count'] = data.count('<bdi')
    values['bdo_count'] = data.count('<bdo')
    values['blockquote_count'] = data.count('<blockquote')
    values['cite_count'] = data.count('<cite')
    values['code_count'] = data.count('<code')
    values['del_count'] = data.count('<del')
    values['dfn_count'] = data.count('<dfn')
    values['em_count'] = data.count('<em>')
    values['font_count'] = data.count('<font')
    values['i_count'] = data.count('<i>') + data.count('<i ')
    values['ins_count'] = data.count('<ins>') + data.count('<ins ')
    values['kbd_count'] = data.count('<kbd')
    values['mark_count'] = data.count('<mark')
    values['meter_count'] = data.count('<meter')
    values['pre_count'] = data.count('<pre>') + data.count('<pre ')
    values['progress_count'] = data.count('<progress')
    values['s_count'] = data.count('<s>') + data.count('<s ')
    values['samp_count'] = data.count('<samp')
    values['small_count'] = data.count('<small')
    values['strong_count'] = data.count('<strong')
    values['sub_count'] = data.count('<sub>') + data.count('<sub ')
    values['sup_count'] = data.count('<sup>') + data.count('<sup ')
    values['time_count'] = data.count('<time')
    values['u_count'] = data.count('<u>') + data.count('<u ')
    values['var_count'] = data.count('<var')
    values['wbr_count'] = data.count('<wbr')
    # Forms and input tags
    values['form_count'] = data.count('<form')
    values['input_count'] = data.count('<input')
    values['textarea_count'] = data.count('<textarea')
    values['button_count'] = data.count('<button')
    values['select_count'] = data.count('<select')
    values['optgroup_count'] = data.count('<optgroup')
    values['option_count'] = data.count('<option')
    values['label_count'] = data.count('<label')
    values['fieldset_count'] = data.count('<fieldset')
    values['legend_count'] = data.count('<legend')
    values['datalist_count'] = data.count('<datalist')
    values['keygen_count'] = data.count('<keygen')
    values['output_count'] = data.count('<output')
    # Images
    values['image_count'] = data.count('<img')
    values['map_count'] = data.count('<map')
    values['area_count'] = data.count('<area')
    values['canvas_count'] = data.count('<canvas')
    values['figcaption_count'] = data.count('<figcaption')
    values['figure_count'] = data.count('<figure')
    # Lists
    values['ul_count'] = data.count('<ul')
    values['ol_count'] = data.count('<ol')
    values['li_count'] = data.count('<li>') + data.count('<li ')
    values['dir_count'] = data.count('<dir')
    values['dl_count'] = data.count('<dl')
    values['dt_count'] = data.count('<dt')
    values['dd_count'] = data.count('<dd')
    values['menu_count'] = data.count('<menu')
    values['menuitem_count'] = data.count('<menuitem') 
    # Styles
    values['style_count'] = data.count('<style')
    values['span_count'] = data.count('<span')
    values['header_count'] = data.count('<header')
    values['footer_count'] = data.count('<footer')
    values['main_count'] = data.count('<main')
    values['section_count'] = data.count('<section')
    values['article_count'] = data.count('<article')
    values['aside_count'] = data.count('<aside')
    values['details_count'] = data.count('<details')
    values['dialog_count'] = data.count('<dialog')
    values['summary_count'] = data.count('<summary')
    values['caption_count'] = data.count('<caption')
    values['th_count'] = data.count('<th>') + data.count('<th ')
    values['thead_count'] = data.count('<thead')
    values['tbody_count'] = data.count('<tbody')
    values['tfoot_count'] = data.count('<tfoot')
    values['col_count'] = data.count('<col')
    values['colgroup_count'] = data.count('<colgroup')
    conn.close()
    return os.path.basename(s3key), values

conn = boto.connect_s3()
# bucket is the name of the S3 bucket where your data resides

b = conn.get_bucket(bucket, validate=False))  
# inkey_root is the S3 'directory' in which your files are located
inkey_root = '3'
keys = b.list(prefix=inkey_root)

key_list = [key.name for key in keys]

key_list.pop(0)

conn.close()

#sc = pyspark.SparkContext('local', 'Whatever')
# Create an RDD from the list of s3 key names to process stored in key_list
file_list = sc.parallelize(key_list)

p = file_list.flatMap(process_data).collect()

with open('3-formatted_html.csv', 'w') as csvfile:
    fieldnames = p[1].keys()
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    headers = {}
    for field in fieldnames:
        headers[field] = field
    writer.writerow(headers)
    for i in range(0, len(p)):
        if i%2 != 0:
            writer.writerow(p[i])


