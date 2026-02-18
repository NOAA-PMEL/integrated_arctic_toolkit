from lxml import etree
from collections import Counter
from pathlib import Path

# class gbifDatasetParser:

#     def __init__(self)

dataset_xml_dir = Path("/home/mule-external/sci-dig/arctic_toolkit/gbif/2026-01-26/dataset")


def see_how_to_flatten():
    """Get the lay of the land and see what is top level, what is nested"""
    tag_counts = Counter()

    for xml_file in dataset_xml_dir.glob("*.xml"):
        tree = etree.parse(xml_file)
        for el in tree.iter():
            tag = el.tag.split('}')[-1]
            tag_counts[tag] += 1

    for tag, count in tag_counts.most_common(50):
        print(f"{tag}: {count}")