# APIs

APIs play a crucial role in Pass Culture's data stack. Their use range from typical data extraction from various sources to exposing our services and data products to other teams within our private network. This section provides an overview of the APIs developed and maintained by the data team, focusing on their exposure, usage, and documentation. It also provides an overview of APIs used to extract data.

## APIs deployed by the data team
There are mainly two services deployed as APIs by the data team.
### 1. Compliance API
This API is integrated in backoffice to enable fraud team to quickly identify published offers with high-potential fraud.

See Data Science page for more details

### 2. Recommendation API
The recommendation API serves the in-app recommendation system. This API is used internally by backend team to integrate calls to recommendation systems once an user connects to the app.

See Data Science page for more details

## Data extraction via API

### 1. Adage
> Adage (Application Dédiée À la Généralisation de l’Éducation artistique et culturelle) is the platform used by the French Ministry of Education to manage cultural and artistic education projects in schools. It allows teachers to propose and monitor cultural activities for students.

From the Adage API, we extract two kinds of data :
1. Informations about cultural structures (siret, department, region, status, ...)
2. Statistics about involved student in cultural projects in France

### 2. Adresse
> API Adresse is developped by French government to easily make an address search.

This API is used to extract approximate coordinates from the address of pass Culture user. From this coordinates, the department and region are retrieved to deliver regional reportings.

### 3. Appsflyer
> AppsFlyer is a mobile attribution and marketing analytics platform that helps businesses track, measure, and optimize user acquisition and engagement across various advertising channels.

From the API, we extract statistics reports that enable marketing team to evaluate the impact of the advertising campaigns throught different channels.

### 4. Batch
> Batch is a marketing tool to automate and monitor push notifications on mobile devices.

> The data extracted from the Batch systems concerns data about campaigns and transactional notifications on mobile devices.

### 5. Brevo
> Brevo is a marketing tool to automate and monitor marketing emails sent to users.

### 6. Contentful
> Contentful is a tool that helps you store and organize app content so it can be easily updated and shown anywhere, like on different websites, apps, or devices.

### 7. DMS
> DMS (Démarches Simplifiées) is a French government platform that allows administrations to digitize and streamline public service procedures, enabling users to submit and process requests online efficiently.

### 8. Apple/Google : App downloads
> The data extracted from Apple store and Play store enables the pass Culture team to follow the volume of downloads of the mobile app.

### 9. Instagram
> Data extracted from Meta systems enables social media teams to monitor the impact of their work on young people's enthusiasm for culture.

### 10. Metabase
> Metabase is an open-source business intelligence tool that allows users to explore, visualize, and share data through a no-code, user-friendly interface.

### 11. Qualtrics
> Qualtrics is an experience management platform that enables organizations to collect, analyze, and act on customer, employee, product, and brand feedback through surveys and advanced analytics.

> Every month, a survey is sent both to young users and to cultural partners to measure the satisfaction on the device. For this purpose, surveys' answers report are exported in the pass Culture system.

### 12. Siren
> Access information on companies and establishments registered in Insee's Sirene inter-administrative directory

> This API enables the extraction of administrative information on cultural structures. This data is used by the legal/conformity team to prevent fraud.

### 13. Tiktok

> Data extracted from Tiktok systems enables social media teams to monitor the impact of their work on young people's enthusiasm for culture.

### 14. Zendesk
> Zendesk is a cloud-based customer service and support platform that helps manage user interactions through ticketing, live chat, knowledge bases, and automation.
