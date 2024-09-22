import xml.etree.ElementTree as ET
from dynamo import init_dynamodb
from dynamo import create_table
from dynamo import save_to_dynamodb

def parse_property_feed(xml_file):
    # Parse the XML file
    tree = ET.parse(xml_file)
    root = tree.getroot()

    properties = []
    print(root)

    # Iterate through each Property element
    for property_elem in root.findall('./Property'):
        prop = property_elem.find('./PropertyID')

        # Get Id, name, email and address
        if prop is not None:
            property_id = prop.find('./Identification').attrib.get('IDValue', '')
            name = prop.find('./MarketingName').text if prop.find('./MarketingName') is not None else ''
            email = prop.find('./Email').text if prop.find('./Email') is not None else ''

            address = prop.find('./Address')
            if address is not None:
                city = address.find('./City').text if address.find('./City') is not None else ''

        # Get number of bedrooms
        bedrooms = None
        floorplan_elem = property_elem.find('./Floorplan')
        if floorplan_elem is not None:
            bedroom_elem = floorplan_elem.find(".//Room[@RoomType='Bedroom']/Count")
            if bedroom_elem is not None:
                bedrooms = bedroom_elem.text
        
        # Only add to the list if the location is "Madison"
        if city == 'Madison':
            properties.append({
                'property_id': property_id,
                'name': name,
                'email': email,
                'num_bedrooms': bedrooms
            })

    return properties

def main():
    xml_file = 'Abodo feed - Marina.xml'
    properties = parse_property_feed(xml_file)

    # Print the extracted properties
    for prop in properties:
        print(f"Property ID: {prop['property_id']}, Name: {prop['name']}, Email: {prop['email']}, Number of Bedrooms: {prop['num_bedrooms']}")


    db = init_dynamodb()
    table = create_table(db)
    save_to_dynamodb(table, properties)

if __name__ == '__main__':
    main()
