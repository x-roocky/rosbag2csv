import re
import rosbag
import csv
import sys

def make_attr_descriptor(topic, message, force=False):
    # 优先缓存
    if not hasattr(make_attr_descriptor, 'attr_descriptors_'):
        make_attr_descriptor.attr_descriptors_ = {}
    if not force and topic in make_attr_descriptor.attr_descriptors_:
        return make_attr_descriptor.attr_descriptors_[topic]
    assert bool(message)
    # 属性名
    attr_name_pattern = re.compile('(\w+):[ ]+[-+\\d\.eExX]+')
    attr_names = re.findall(attr_name_pattern, message)
    attr_names = sorted(set(attr_names), key=attr_names.index)
    # 属性提取pattern
    attr_pattern = re.compile(''.join([f'{i}:[ ]+(?P<{i}>[-+\\d\.eE]+).*?' for i in attr_names]), re.DOTALL)
    # 缓存并返回结果
    make_attr_descriptor.attr_descriptors_[topic] = (attr_names, attr_pattern)
    return make_attr_descriptor.attr_descriptors_[topic]

def msg_to_attr(topic, timestamp, message):
    attr_names, attr_pattern = make_attr_descriptor(topic, message)

    try:
        results = re.search(attr_pattern, message)
        attrs = [float(results.group(name)) for name in attr_names]
        return [topic, timestamp] + attrs
    except Exception as e:
        print(e)
        return [topic, timestamp] + [str('ERROR'), str(message), str(attr_names), str(attr_pattern)]

def msg_header(topic):
    attr_names, _ = make_attr_descriptor(topic, None)
    return ['rosbag_topic', 'rosbag_timestamp'] + attr_names

def bag_to_attrs(path, topics):
    bags = rosbag.Bag(path, 'r').read_messages()
    attrs = [msg_to_attr(str(topic), timestamp, str(message))
        for topic, message, timestamp in bags if str(topic) in topics]
    return attrs

def attrs_to_csv(path, header, attrs):
    with open(path, 'w') as output_file:
        output_csv = csv.writer(output_file)
        output_csv.writerow(header)
        output_csv.writerows(attrs)

def bag_to_csv(input_path, topic_groups, callback=None):
    all_topics = list(set([value for values in topic_groups.values() for value in values]))
    all_attrs = bag_to_attrs(input_path, all_topics)
    for name in topic_groups:
        attrs = [attr for attr in all_attrs if attr[0] in topic_groups[name]]
        output_path = f'{input_path}.{name}.csv'
        attrs_to_csv(output_path, msg_header(topic_groups[name][0]), attrs)
        if callback:
            callback(output_path, attrs)

if __name__ == '__main__':
    # 检测参数个数
    args = sorted(set(sys.argv[1:]), key=sys.argv.index)
    if len(args) < 2:
        print(f'sh: python3 {sys.argv[0]} [topic1, topic2, ...] "1.bag" "2.bag" ...')
        print(f'eg: python3 {sys.argv[0]} "/sensor/gnss_rtk" ./x.bag')
        print(f'eg: python3 {sys.argv[0]} "/sensor/gnss_rtk" ./xx/x1.bag ./xx/x2.bag ./xx/x3.bag')
        print(f'eg: python3 {sys.argv[0]} "/sensor/gnss_rtk, /sensor/gnss_rtk_tbox" ./xx/*.bag')
        exit(-1)

    # topic_groups提取
    topics = [ t.strip() for t in sys.argv[1].split(',') ]
    topic_groups = { t.strip('/').replace('/', '_'): [t] for t in topics }
    args = args[1:]

    # 解析bag中数值型信号为csv
    for input_path in args:
        print(f'input_path: {input_path}')
        bag_to_csv(input_path, topic_groups, lambda path, _: print(f'\toutput_path: {path}'))
