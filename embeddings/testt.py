dictt={
    "embedding_extract_from" : "offers" , 
    "features":[{
        "name": "offer_name",
        "type": "text"
    },
    {
        "name": "offer_description",
        "type": "text"
    },
    {
        "name": "offer_image",
        "type": "image"
    },
    ]
}
tt=[features["name"] for features in dictt["features"] if features["type"]=='text']
print(tt)

