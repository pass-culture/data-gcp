{% docs __overview__ %}

![Pass Culture Logo](https://upload.wikimedia.org/wikipedia/commons/4/4f/Logo_du_Pass_Culture.png)

# Pass Culture Data Project

The Pass Culture data project is a comprehensive data management and analytics platform designed to support cultural offers. It leverages dbt to transform raw data into structured formats, facilitating efficient querying and reporting. The project organizes data into marts, focusing on key areas such as users, offers, bookings, stock, and deposits.

- **[mrt_global__user](/#!/model/model.data_gcp_dbt.mrt_global__user)**: Aggregates user-related data, including demographics, activity, and financial interactions. It tracks user engagement, marketing preferences, and financial transactions.

- **[mrt_global__offer](/#!/model/model.data_gcp_dbt.mrt_global__offer)**: Contains detailed information about cultural offers, including identifiers, descriptions, categories, and availability. It ensures only approved offers are available for users.

- **[mrt_global__booking](/#!/model/model.data_gcp_dbt.mrt_global__booking)**: Tracks user bookings, including details about the booking status, amount, and associated offers. It provides comprehensive insights into user interactions with offers.

- **[mrt_global__stock](/#!/model/model.data_gcp_dbt.mrt_global__stock)**: Manages inventory levels for offers, tracking availability, pricing, and booking limits. It ensures accurate availability information.

- **[mrt_global__deposit](/#!/model/model.data_gcp_dbt.mrt_global__deposit)**: Handles financial transactions related to offers, including deposit amounts, types, and user associations. It tracks spending and booking activity.

The project supports various analytical needs, enabling data-driven decision-making and enhancing the cultural experience for users.

## Useful Links

- [Pass Culture Website](https://pass.culture.fr)
- [GitHub Repository](https://github.com/pass-culture/data-gcp)
- [Notion Project Page](https://www.notion.so/passcultureapp/P-le-Data-2f13949260094b9bb3d91ec90289d8bd)
- [Data Engineering Board](https://www.notion.so/passcultureapp/b0160617ae6b471bb252e459793351dd?v=6a44d0b261c54c7a993431eb13bab35a)
- [Data Analytics Board](https://www.notion.so/passcultureapp/Tasks-data-8891c4dc2c554f7986deaac579663636)


## Contact

- [Contact Us](https://www.notion.so/passcultureapp/Data-Nous-contacter-815c3cbec70748caab19cb5d8ca7870a)

{% enddocs %}
