The `mrt_global__offerer_tag` table lists all tags assigned by the internal pass Culture teams related to entities (offerer_id).

These tags aim to better qualify and segment cultural partners for purposes such as sectoral monitoring, partner accounting, or supporting teams responsible for the accreditation process. Each row corresponds to a tag applied to an entity. A similar tag can be applied to several entities, an entity can have several tags.

## Table description

| name                   | data_type | description                                                                                |
| ---------------------- | --------- | ------------------------------------------------------------------------------------------ |
| offerer_tag_mapping_id |           | The unique identifier for the offerer tag mapping.                                         |
| offerer_id             |           | Unique identifier of the offerer.                                                          |
| tag_id                 |           | The unique identifier for the tag.                                                         |
| tag_name               |           | The name (as displayed in pass Culture backoffice) of the tag associated with the offerer. |
| tag_label              |           | The label (for display purposes) of the tag associated with the offerer.                   |
| tag_description        |           | The description of the tag associated with the offerer.                                    |
| tag_category_id        |           | The unique identifier for the tag category.                                                |
| tag_category_name      |           | The name (as displayed in pass Culture backoffice) of the tag category.                    |
| tag_category_label     |           | The label (for display purposes) of the tag category.                                      |
