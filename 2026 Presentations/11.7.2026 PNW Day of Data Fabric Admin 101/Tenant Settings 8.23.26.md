---
layout: Conceptual
title: Tenant settings index - Microsoft Fabric | Microsoft Learn
canonicalUrl: https://learn.microsoft.com/en-us/fabric/admin/tenant-settings-index
breadcrumb_path: /fabric/breadcrumb/toc.json
uhfHeaderId: MSDocsHeader-Fabric
show_latex: false
feedback_system: Standard
feedback_help_link_url: https://community.fabric.microsoft.com/powerbi
feedback_help_link_type: ask-the-community
feedback_product_url: https://ideas.fabric.microsoft.com/?forum=bbb1936d-44f9-ed11-8849-000d3a4ef41d&category=023c0612-44be-ed11-83ff-00224827e905
ms.service: fabric
author: msmimart
ms.author: mimart
ms.subservice: admin
search.app:
- fabric-powerbi-docs
- capaedac-PowerBI
- powerbi
- Powerbi-docs
description: A comprehensive index of Microsoft Fabric tenant settings in the Fabric admin portal, including descriptions and links to relevant documentation.
ms.topic: reference
ms.collection: ce-skilling-ai-copilot
ms.update-cycle: 180-days
ms.date: 2026-08-13T00:00:00.0000000Z
locale: en-us
document_id: 124d960d-8743-1328-0331-b9dc7919edf8
document_version_independent_id: 124d960d-8743-1328-0331-b9dc7919edf8
updated_at: 2026-08-14T05:06:00.0000000Z
original_content_git_url: https://github.com/MicrosoftDocs/fabric-docs-pr/blob/live/docs/admin/tenant-settings-index.md
gitcommit: https://github.com/MicrosoftDocs/fabric-docs-pr/blob/ba2d72c5e406c77a79f6f5ea8f155e3cca65e1fd/docs/admin/tenant-settings-index.md
git_commit_id: ba2d72c5e406c77a79f6f5ea8f155e3cca65e1fd
site_name: Docs
depot_name: MSDN.fabric-docs
page_type: conceptual
toc_rel: toc.json
pdf_url_template: https://learn.microsoft.com/pdfstore/en-us/MSDN.fabric-docs/{branchName}{pdfName}
word_count: 8047
asset_id: admin/tenant-settings-index
moniker_range_name: 
monikers: []
item_type: Content
source_path: docs/admin/tenant-settings-index.md
cmProducts:
- https://authoring-docs-microsoft.poolparty.biz/devrel/d3197845-b4ce-44c6-a237-cd4be160e76c
- https://microsoft-devrel.poolparty.biz/DevRelOfferingOntology/e7cd09c2-9e9a-4f0d-b160-c15783f45b1f
- https://authoring-docs-microsoft.poolparty.biz/devrel/63959238-cb90-4871-a33d-4a5519097e47
spProducts:
- https://authoring-docs-microsoft.poolparty.biz/devrel/aea905fb-0a9d-4d46-b30f-e9cbaf772d1b
- https://microsoft-devrel.poolparty.biz/DevRelOfferingOntology/1d337399-0ca4-4b4c-acef-8df37820e018
- https://authoring-docs-microsoft.poolparty.biz/devrel/78d87f42-5582-4a6b-90be-7db2f12b34e6
platformId: bd66bb68-7b9c-fad4-2907-399497b6f8b0
---

# Tenant settings index - Microsoft Fabric | Microsoft Learn

This article lists all Fabric tenant settings, along with a brief description of each, and links to relevant documentation, if available. For more information about tenant settings in general, see [About tenant settings](about-tenant-settings).

If you want to get to the tenant settings in the Fabric portal, see [How to get to the tenant settings](about-tenant-settings#how-to-get-to-the-tenant-settings).

## [Microsoft Fabric](service-admin-portal-microsoft-fabric-tenant-settings)

| Setting name | Description |
| --- | --- |
| [Users can create Fabric items](fabric-switch) | Users can use production-ready features to create Fabric items. Turning off this setting doesn't impact users' ability to create Power BI items. This setting can be managed at both the tenant and the capacity levels. [Learn More](https://go.microsoft.com/fwlink/?linkid=2247554) |
| Users can create Ontology (preview) items | Users can create ontologies to unify enterprise semantics across data, models and logic to operationalize decision intelligence with context-aware AI agents. [Learn More](https://aka.ms/ontologyitem-overview) |
| [Users can create Digital Twin Builder (preview) items](../real-time-intelligence/digital-twin-builder/overview) | Users can create digital twin builder items to build comprehensive digital twins of real world environments and processes, to enable big-picture data analysis and drive operational efficiency. |
| [Product Feedback](../fundamentals/feedback) | This setting allows Microsoft to prompt users for feedback through in-product surveys within Microsoft Fabric and Power BI. Microsoft will use this feedback to help improve product features and services. User participation is voluntary. [Learn More](https://go.microsoft.com/fwlink/?linkid=2272840) |
| [Users can be informed of upcoming conferences featuring Microsoft Fabric when they are logged in to Fabric](service-admin-portal-microsoft-fabric-tenant-settings#users-can-be-informed-of-upcoming-conferences-featuring-microsoft-fabric-when-they-are-logged-in-to-fabric) | Attending conferences can help your data teams learn in-depth about how to best use the Fabric platform for your business needs and build professional relationships with community members, Microsoft engineering and product teams, and the Fabric Customer Advisory Team (CAT). These conferences may be organized and hosted by third parties. [Learn More](https://go.microsoft.com/fwlink/?linkid=2306100) |
| ML models can serve real-time predictions from API endpoints (preview) | With this setting on, users can create real-time predictions from model and version endpoints. Even with real-time endpoints turned off, batch predictions can still be generated. [Learn More](https://aka.ms/MLModelEndpointsLearnMore) |
| Detect anomalies in Real-Time Intelligence (Preview) | This setting allows users to use statistical detection algorithms to detect anomalies in real-time data. [Learn More](https://aka.ms/AD_docs) |
| Enable Event Schema Sets in Real-Time Hub (preview) | This setting allows users to define the structure and format of events through schemas, enabling data quality and validation with group-level access control. [Learn More](https://aka.ms/AD_docs) |
| Users can create dbt job items (preview) | Users can import, author and execute dbt (data build tool) projects directly within Fabric. This allows users to access a powerful transformation engine that connects seamlessly with SQL-based workflows without having to set up a CLI environment. [Learn More](https://aka.ms/dbtjob_docs) |
| All Power BI users can see "Set alert" button to create Fabric Activator alerts | When enabled, all Power BI users will see the "Set alert" button in reports. However, only users with permission to create Fabric items can actually set up Fabric Activator alerts, which send real-time notifications based on predefined data conditions. [Learn More](https://go.microsoft.com/fwlink/?linkid=2331953) |
| Enable new mirrored catalog items (preview) | Turn on this setting to allow users to create mirrored catalog items in Fabric, to mirror tables from external catalogs into OneLake through shortcuts. This only affects mirrored catalog item types that are in a Preview state. [Learn More](https://aka.ms/OneLakeMirroredCatalogDocs) |
| Enable Fabric App Items (preview) | Users can create Fabric apps which is a Fabric-native suite of services for building intelligent applications for front end and backend of your application. It allows developers and AI agents to create and operate complete app backends directly on Microsoft Fabric. Fabric Apps unifies operational data, analytics, and AI while meeting enterprise security and governance requirements. [Learn More](https://go.microsoft.com/fwlink/?linkid=2353418) |
| Enable anonymous data access for Fabric Apps (Preview) | This setting allows users to enable anonymous data access for Fabric Apps that are intended for public consumption. Admins can limit this capability to specific users or security groups. |
| Users can create deployment plan (preview) items | Turn on this setting to allow users to define the order, dependencies, and actions for deploying workspace items across environments. [Learn More](https://go.microsoft.com/fwlink/?linkid=2301210) |
| Users can access the Database hub (preview) | When turned on, users can monitor configuration and performance metrics across their Azure and Fabric databases. [Learn More](https://aka.ms/database-hub-learn-more) |
| Users can create Azure Databricks Storage items (preview) | Users can create Azure Databricks storage item that uses OneLake for storage. |
| Enable sending Operations Agents observability data to Agent 365 (preview) | Allow Microsoft Fabric Operation Agent data to be processed and stored by Agent 365, such as information about the Fabric item for this agent, the user ID of the creator of the agent, the Entra Agent ID that the agent is operating with, and the prompts and tools that it uses. This will help Agent 365 administrators monitor what Fabric agents are being created, where they are being used and more. When this setting is turned on, data sent to Agent 365 can be processed and/or stored outside your capacity's geographic region, compliance boundary, or national cloud instance. [Learn More](https://go.microsoft.com/fwlink/?LinkId=2365600) |
| Insights for operations agents (preview) | Users can view operational insights for events generated by operations agents.These insights help users understand what happened and why by showing summarized findings and key observations based on agent activity and underlying data. [Learn More](https://aka.ms/OA_RCA_learnmore)Turning on this setting can help your organization improve issue triage by identifying possible causes behind events, understand the context behind agent recommendations, and increase transparency into how agents analyze and act on data. |
| Fabric data agents can send operational metadata for observability in Microsoft Foundry | Agent creators can use operational metadata to monitor agent health, detect issues, and understand how their published agents are used across applications and services. These insights help agent creators improve agent reliability and performance.When this setting is turned on, operational metadata from Fabric data agents is processed and stored in a configured Application Insights resource. This data includes agent display names, data source names, reasoning step IDs, and conversation IDs. The Application Insights resource that processes and stores this data may be located outside your tenant's geographic region, compliance boundary, or national cloud instance.Accessing the operational metadata requires a role with read permissions (such as Monitoring Reader or Log Analytics Reader) on the configured Application Insights resource. Users with this access can view the data in Application Insights. Users can also view the data through a Foundry project that connects to the configured Application Insights resource. Access through Foundry requires role-based access to the Foundry project (Foundry User or higher) in addition to read permissions on the Application Insights resource. Access to a Foundry project alone doesn’t include access to the telemetry data. [Learn More](https://aka.ms/Fabric/Data-Agent-Foundry-Observability) |

## [Help and support settings](service-admin-portal-help-support)

| Setting name | Description |
| --- | --- |
| [Publish "Get Help" information](service-admin-portal-help-support#publish-get-help-information) | Users in the organization can go to internal help and support resources from the Power BI help menu. |
| Receive email and Teams notifications for service outages or incidents | Mail-enabled security groups will receive email and Teams notifications if this tenant is impacted by a service outage or incident. |
| Receive In-product notifications for service outages or incidents | Mail-enabled security groups will receive In-product notifications if this tenant is impacted by a service outage or incident. [Learn More](https://go.microsoft.com/fwlink/?linkid=2347823) |
| [Users can try Microsoft Fabric paid features](service-admin-portal-help-support#users-can-try-microsoft-fabric-paid-features) | When users sign up for a Microsoft Fabric trial, they can try Fabric paid features for free for 60 days from the day they signed up. [Learn More](https://go.microsoft.com/fwlink/?linkid=2227896) |
| [Show a custom message before publishing reports](service-admin-portal-help-support#show-a-custom-message-before-publishing-reports) | When people attempt to publish a report, they'll see a custom message before it gets published. |

## Domain management settings

| Setting name | Description |
| --- | --- |
| [Allow tenant and domain admins to override workspace assignments (preview)](service-admin-portal-domain-management-settings#allow-tenant-and-domain-admins-to-override-workspace-assignments-preview) | Tenant and domain admins can reassign workspaces that were previously assigned to one domain to another domain. |

## [Workspace settings](portal-workspace)

| Setting name | Description |
| --- | --- |
| [Create workspaces](portal-workspace#create-workspaces) | Users in the organization can create app workspaces to collaborate on dashboards, reports, and other content. Even if this setting is disabled, a workspace will be created when a template app is installed. |
| [Use semantic models across workspaces](portal-workspace#use-semantic-models-across-workspaces) | Users in the organization can use semantic models across workspaces if they have the required Build permission. |
| [Block users from reassigning personal workspaces (My Workspace)](portal-workspace#block-users-from-reassigning-personal-workspaces-my-workspace) | Turn on this setting to prevent users from reassigning their personal workspaces (My Workspace) from Premium capacities to shared capacities. [Learn More](https://aka.ms/RestrictMyFolderCapacity) |
| [Define workspace retention period](workspace-retention#set-up-the-retention-period-for-deleted-collaborative-workspaces) | Turn on this setting to define a retention period during which you can restore a deleted workspace and recover items in it. At the end of the retention period, the workspace is permanently deleted. By default, workspaces are always retained for a minimum of 7 days before they're permanently deleted.Turn off this setting to accept the minimum retention period of 7 days. After 7 days the workspace and items in it will be permanently deleted.Enter the number of days to retain a workspace before it's permanently deleted. My Workspace workspaces will be retained for 30 days automatically. Other workspaces can be retained for up to 90 days. |
| Automatically convert and store reports using Power BI enhanced metadata format (PBIR) (preview) | Enable this setting to automatically convert reports to PBIR format after editing and save them in the workspace.When this is activated, new reports will also be created in PBIR format.PBIR format provides source control-friendly file structures, enhancing co-development and boosting development efficiency for Power BI reports. [Learn More](https://go.microsoft.com/fwlink/?linkid=2263123) |
| Fabric item recovery | Once item recovery is enabled, deleted items are retained for a minimum of 7 days. You can specify a longer retention period for a maximum of 90 days. At the end of the retention period, the items are permanently deleted.Note: Some Fabric items do not support item recovery. [Learn more](https://aka.ms/FabricItemRecovery)If you leave this setting turned off, deleting items will be a permanent action.Turn on this setting to enable the deleted items to be retained for a defined period.Enter the number of days to retain items before they're permanently deleted. |

## [Information protection](service-admin-portal-information-protection)

| Setting name | Description |
| --- | --- |
| [Allow users to apply sensitivity labels for content](service-admin-portal-information-protection#allow-users-to-apply-sensitivity-labels-for-content) | With this setting enabled, Microsoft Purview Information Protection sensitivity labels published to users by your organization can be applied. All [prerequisite steps](https://go.microsoft.com/fwlink/?linkid=2144840) must be completed before enabling this setting.Important: Sensitivity-label-based access control for Fabric and Power BI data and content is only enforced in the tenant where the labels were applied, in Power BI Desktop (.pbix) files, and in Excel, PowerPoint, and PDF files generated via [supported export paths](https://go.microsoft.com/fwlink/?linkid=2141966). Sensitivity-label-based access control is not supported in cross-tenant scenarios, such as [external data sharing](https://go.microsoft.com/fwlink/?linkid=2241057), or in any other export scenario, such as export to .csv or .txt formats. For more information, see [Information protection in Microsoft Fabric: Access control](https://go.microsoft.com/fwlink/?linkid=2300724).Note: Sensitivity label settings, such as encryption and content marking for files and emails, are not applied to content in Fabric. [Learn More](https://go.microsoft.com/fwlink/?linkid=2143053). Encryption is applied to content in [supported export paths](https://go.microsoft.com/fwlink/?linkid=2141966). |
| [Apply sensitivity labels from data sources to their data in Power BI](/en-us/power-bi/enterprise/service-security-sensitivity-label-inheritance-from-data-sources) | Only sensitivity labels from supported data sources will be applied. Please see the documentation for details about supported data sources and how their sensitivity labels are applied in Power BI. [Learn about supported data sources](https://go.microsoft.com/fwlink/?linkid=2149746) |
| [Automatically apply sensitivity labels to downstream content](/en-us/power-bi/enterprise/service-security-sensitivity-label-downstream-inheritance) | With this setting enabled, whenever a sensitivity label is changed or applied to Fabric content, the label will also be applied to its eligible downstream content. [Learn More](https://go.microsoft.com/fwlink/?linkid=2153971) |
| [Allow workspace admins to override automatically applied sensitivity labels](/en-us/power-bi/enterprise/service-security-sensitivity-label-change-enforcement#relaxations-to-accommodate-automatic-labeling-scenarios) | With this setting enabled, workspace admins can change or remove sensitivity labels that were applied automatically by Fabric, for example, as a result of label inheritance. [Learn More](https://go.microsoft.com/fwlink/?linkid=2154646) |
| [Restrict content with protected labels from being shared via link with everyone in your organization](service-admin-portal-information-protection#restrict-content-with-protected-labels-from-being-shared-via-link-with-everyone-in-your-organization) | This setting will prevent content with protection settings in the sensitivity label from being shared via link with everyone in your organization. [Learn More](https://go.microsoft.com/fwlink/?linkid=2160554) |
| [Domain admins can set default sensitivity labels for their domains (preview)](../governance/domain-default-sensitivity-label) | Domain admins can set a default sensitivity label for their domains. The label they set will override your organization's default labels in Microsoft Purview, as long as it has a higher priority than the existing default labels set for your tenant. A domain's default label will automatically apply to new Fabric items created within the domain. Reports, semantic models, dataflows, dashboards, scorecards, and some additional item types aren't currently supported. [Learn More](https://go.microsoft.com/fwlink/?linkid=2298803) |
| Allow Microsoft Purview to secure AI interactions | Allow Microsoft Purview to access, process, and store prompts and responses-including metadata-for data security and compliance outcomes such as sensitive info type (SIT) classification, reporting in Microsoft Purview Data Security Posture Management for AI, Audit, Insider Risk Management, Communication Compliance, and eDiscovery. Note: This is a Microsoft Purview paid capability and is not included in the Copilot in Fabric pricing. [Learn More](https://go.microsoft.com/fwlink/?linkid=2296824) |

## [Export and sharing settings](service-admin-portal-export-sharing)

| Setting name | Description |
| --- | --- |
| [External data sharing](../governance/external-data-sharing-overview) | Users can share a read-only link to data stored in OneLake with collaborators outside your organization. When you grant them permission to do so, users can share a link to data in lakehouses and additional Fabric items. Collaborators who receive the link can view, build on, and share the data both within and beyond their own Fabric tenants, using their organization's licenses and capacities. [Learn More](https://go.microsoft.com/fwlink/?linkid=2264947) |
| [Users can accept external data shares](../governance/external-data-sharing-overview) | Users can accept a read-only link to data from another organization's Fabric tenant. Users who accept an external share link can view, build on, and share the data, both inside and outside of your organization's tenant. For more information about the security limitations of this preview feature, view the feature documentation. [Learn More](https://go.microsoft.com/fwlink/?linkid=2264947) |
| [Guest users can access Microsoft Fabric](../enterprise/powerbi/service-admin-entra-b2b) | Guest users who've been added to your Microsoft Entra directory can access Microsoft Fabric and any Fabric items they have permissions to. [Learn More](https://go.microsoft.com/fwlink/?linkid=2257019) |
| [Users can invite guest users to collaborate through item sharing and permissions](../enterprise/powerbi/service-admin-entra-b2b#invite-guest-users) | Users can collaborate with people outside the organization by sharing Fabric items with them and granting them permission to access those items. After external users accept an invitation, they're added to your Microsoft Entra directory as guest users. [Learn More](https://go.microsoft.com/fwlink/?linkid=2256936) |
| [Guest users can browse and access Fabric content](../enterprise/powerbi/service-admin-entra-b2b) | Users can invite guest users to browse and request access to Fabric content. [Learn More](https://go.microsoft.com/fwlink/?linkid=2038485) |
| [Users can see guest users in lists of suggested people](service-admin-portal-export-sharing#users-can-see-guest-users-in-lists-of-suggested-people) | With this setting on, users will see both users in your organization and guest users who've been added to your Microsoft Entra directory in lists of suggested people. With this setting off, users will see only users in your organization.Users can still share items with guests by providing their full email address. [Learn More](https://go.microsoft.com/fwlink/?linkid=2256937) |
| [Publish to web](service-admin-portal-export-sharing#publish-to-web) | People in your org can publish public reports on the web. Publicly published reports don't require authentication to view them.Go to [Embed codes](service-admin-portal-embed-codes) in the admin portal to review and manage public embed codes. If any of the codes contain private or confidential content remove them.Review embed codes regularly to make sure no confidential information is live on the web. [Learn more about Publish to web](https://go.microsoft.com/fwlink/?LinkID=859242) |
| Copy and paste visuals | Users in the organization can copy visuals from a tile or report visual and paste them as static images into external applications. |
| [Export to Excel](/en-us/power-bi/visuals/power-bi-visualization-export-data) | Users in the organization can export the data from a visualization or paginated report to an Excel file. [Learn More](https://go.microsoft.com/fwlink/?linkid=2143053) |
| [Export to .csv](/en-us/power-bi/paginated-reports/report-builder/export-csv-file-report-builder) | Users in the organization can export data from a tile, visualization, or paginated report to a .csv file. [Learn More](https://go.microsoft.com/fwlink/?linkid=2143055) |
| [Download reports](/en-us/power-bi/create-reports/service-export-to-pbix) | Users in the organization can download .pbix files and paginated reports. [Learn More](https://go.microsoft.com/fwlink/?linkid=2143210) |
| [Users can work with semantic models in Excel using a live connection](/en-us/power-bi/collaborate-share/service-analyze-in-excel) | Users can export data to Excel from a report visual or semantic model, or export a semantic model to an Excel workbook with Analyze in Excel, both options with a live connection to the XMLA endpoint. [Learn More](https://go.microsoft.com/fwlink/?linkid=2143211) |
| [Export reports as PowerPoint presentations or PDF documents](service-admin-portal-export-sharing#export-reports-as-powerpoint-presentations-or-pdf-documents) | Users in the organization can export reports as PowerPoint files or PDF documents. |
| Export reports as MHTML documents | Users in the organization can export Paginated reports as MHTML documents. |
| [Export reports as Word documents](/en-us/power-bi/paginated-reports/report-builder/export-microsoft-word-report-builder) | Users in the organization can export Paginated reports as Word documents. |
| [Export reports as XML documents](/en-us/power-bi/paginated-reports/report-builder/export-xml-report-builder) | Users in the organization can export Paginated reports as XML documents. |
| [Export reports as image files](/en-us/power-bi/paginated-reports/report-builder/export-image-file-report-builder) | Users in the organization can use the export report to file API to export reports as image files. |
| [Print dashboards and reports](/en-us/power-bi/consumer/end-user-print) | Users in the organization can print dashboards and reports. |
| [Certification](/en-us/power-bi/admin/service-admin-setup-certification) | Choose whether people in your org or specific security groups can certify items (like apps, reports, or datamarts) as trusted sources for the wider organization.Note: When a user certifies an item, their contact details will be visible along with the certification badge. |
| [Endorse master data](../governance/endorsement-overview) | Choose whether people in your org or specific security groups can endorse items (like lakehouses, warehouses, or datamarts) as one of the core sources for your organization's data records. [Learn More](https://go.microsoft.com/fwlink/?linkid=2265090)Note: When someone endorses an item as master data, their name and email will show with the endorsement badge. |
| [Users can set up email subscriptions](/en-us/power-bi/collaborate-share/end-user-subscribe) | Users can create email subscriptions to reports and dashboards. |
| [B2B guest users can set up and be subscribed to email subscriptions](service-admin-portal-export-sharing#b2b-guest-users-can-set-up-and-be-subscribed-to-email-subscriptions) | B2B guest users can set up and be subscribed to email subscriptions. B2B guest users are external users that have been added to your Microsoft Entra ID. Turn this setting off to prevent B2B guest users from setting up or being subscribed to email subscriptions. [Learn More](https://go.microsoft.com/fwlink/?linkid=2256373) |
| [Users can send email subscriptions to external users](service-admin-portal-export-sharing#users-can-send-email-subscriptions-to-external-users) | Users can send email subscriptions to external users. External users are users you've not added to your Microsoft Entra ID. Turn this setting off to prevent users from subscribing external users to email subscriptions. [Learn More](https://go.microsoft.com/fwlink/?linkid=2256884) |
| [Featured content](/en-us/power-bi/collaborate-share/service-featured-content) | Users in the organization can promote their published content to the Featured section of Power BI Home. |
| [Allow shareable links to grant access to everyone in your organization](service-admin-portal-export-sharing#allow-shareable-links-to-grant-access-to-everyone-in-your-organization) | This setting will grant access to anyone in your organization with the link. It won't work for external users. [Learn More](https://go.microsoft.com/fwlink/?linkid=2153738) |
| [Enable Microsoft Teams integration](/en-us/power-bi/collaborate-share/service-collaborate-microsoft-teams) | This setting allows people in the organization to access features associated with the Microsoft Teams and Power BI integration. This includes launching Teams experiences from the Power BI service like chats, the Power BI app for Teams, and receiving Power BI notifications in Teams. To completely enable or disable Teams integration, work with your Teams admin. |
| Install Power BI app for Microsoft Teams automatically | The Power BI app for Microsoft Teams is installed automatically for users when they use Microsoft Fabric. The app is installed for users if they have Microsoft Teams and the Power BI app is allowed in the Teams Admin Portal. When the app is installed, users receive notifications in Teams and can more easily discover and collaborate with colleagues. The Power BI app for Teams provides users with the ability to open all Fabric content. [Learn More](https://go.microsoft.com/fwlink/?linkid=2171149). |
| [Enable Power BI add-in for PowerPoint](service-admin-portal-export-sharing#enable-power-bi-add-in-for-powerpoint) | Let people in your org embed Power BI data into their PowerPoint presentations. This integration requires that your organization's Microsoft Office admin has enabled support for add-ins. |
| [Allow DirectQuery connections to Power BI semantic models](service-admin-portal-export-sharing#allow-directquery-connections-to-power-bi-semantic-models) | DirectQuery connections allow users to make changes to existing semantic models or use them to build new ones. [Learn More](https://go.microsoft.com/fwlink/?linkid=2179788) |
| [Guest users can work with shared semantic models in their own tenants](/en-us/power-bi/collaborate-share/service-dataset-external-org-share-admin#allow-guest-users-to-work-with-shared-datasets-in-their-own-tenants) | Authorized guest users can discover semantic models shared with them in the OneLake data hub (in Power BI Desktop), and then work with these semantic models in their own Power BI tenants. |
| [Allow specific users to turn on external data sharing](/en-us/power-bi/collaborate-share/service-dataset-external-org-share-admin#allow-specific-users-to-turn-on-external-data-sharing) | Turn off this setting to prevent all users from turning on external data sharing. If this setting is on, all or specific users can turn on the external data sharing option, allowing them to share data with authorized guest users. Authorized guest users can then discover, connect to, and work with these shared semantic models in their own Power BI tenants. |
| Users with read or write permission can download data from notebooks | With this setting on, users with read or write permission can download or export data from tables in notebook outputs. [Learn More](https://go.microsoft.com/fwlink/?linkid=2299416) |

## [Discovery settings](service-admin-portal-discovery)

| Setting name | Description |
| --- | --- |
| [Make promoted content discoverable](/en-us/power-bi/collaborate-share/service-discovery) | Allow users in this org who can promote content to make content they promote discoverable by users who don't have access to it. [Learn More](https://go.microsoft.com/fwlink/?linkid=2156467) |
| [Make certified content discoverable](/en-us/power-bi/collaborate-share/service-discovery) | Allow users in the org who can certify content to make content they certify discoverable by users who don't have access to it. [Learn More](https://go.microsoft.com/fwlink/?linkid=2156467) |
| [Discover content](../governance/onelake-catalog-overview) | Allow users to find and request access to content they don't have access to if it was made discoverable by its owners. [Learn More](https://go.microsoft.com/fwlink/?linkid=2156467) |

## App settings

| Setting name | Description |
| --- | --- |
| [Create template organizational apps](/en-us/power-bi/connect-data/service-template-apps-create) | Users in the organization can create template apps that use semantic models built on one data source in Power BI Desktop. |
| [Push apps to end users](/en-us/power-bi/collaborate-share/service-create-distribute-apps#automatically-install-apps-for-end-users) | Users can share apps directly with end users without requiring installation from AppSource. |
| [Publish apps to the entire organization](/en-us/power-bi/collaborate-share/service-create-distribute-apps#publish-the-app-to-your-entire-organization) | Users in the organization can publish apps to the entire organization. |

## [Integration settings](service-admin-portal-integration)

| Setting name | Description |
| --- | --- |
| [Allow XMLA endpoints and Analyze in Excel with on-premises semantic models](/en-us/power-bi/collaborate-share/service-analyze-in-excel) | Users in the organization can use Excel to view and interact with on-premises Power BI semantic models. This also allows connections to XMLA endpoints. |
| [Semantic Model Execute Queries REST API](/en-us/rest/api/power-bi/datasets/execute-queries) | Users in the organization can query semantic models by using Data Analysis Expressions (DAX) through Power BI REST APIs. |
| Users can use the Power BI Model Context Protocol server endpoint (preview) | When enabled, the Power BI Model Context Protocol (MCP) server endpoint allows users to connect MCP clients to Power BI to use MCP tools to interact with their permissioned Power BI artifacts. GitHub Copilot in Visual Studio will operate by default; configure other MCP clients with service principals. To allow service principals to connect to the MCP Server, turn on "Service principals can call Fabric public APIs." [Learn More](https://go.microsoft.com/fwlink/?linkid=2338916) |
| [Use ArcGIS Maps for Power BI](/en-us/power-bi/visuals/power-bi-visualizations-arcgis) | Users in the organization can use the ArcGIS Maps for Power BI visualization provided by Esri. |
| [Use global search for Power BI](/en-us/power-bi/consumer/end-user-search-sort) | Turn on this setting to let users use the global search bar at the top of the page. |
| [Users can use the Azure Maps visual](/en-us/azure/azure-maps/power-bi-visual-get-started) | With this setting on, users can create and view the Azure Maps visual. Your data may be temporarily stored and processed by Microsoft for essential services, including translating location names into latitudes and longitudes. Use of Azure Maps is subject to the following [Terms of use](https://go.microsoft.com/fwlink/?linkid=2271924). |
| Data sent to Azure Maps can be processed outside your tenant's geographic region, compliance boundary, or national cloud instance | Azure Maps services are currently not available in all regions and geographies. With this setting on, data sent to Azure Maps can be processed in a region where the service is available, which might be outside your tenant's geographic region, compliance boundary, or national cloud instance. [Learn More](https://go.microsoft.com/fwlink/?linkid=2289253) |
| [Data sent to Azure Maps can be processed by Microsoft Online Services Subprocessors](https://go.microsoft.com/fwlink/?linkid=2289928) | Some Azure Maps visual services, including the selection tool and the processing of location names within some regions, may require mapping capabilities provided in part by Microsoft Online Services subprocessors. Microsoft shares only necessary data with these Microsoft Online Services subprocessors, who may access data only to deliver the functions in support of online services that Microsoft has engaged them to provide and are prohibited from using data for any other purpose. Microsoft does not share the name of the customer or end user who submits the query. This feature is non-regional and the queries you provide may be stored and processed in the United States or any other country in which Microsoft or its subprocessors operate. [Learn More](https://go.microsoft.com/fwlink/?linkid=2289928) |
| [Map and filled map visuals](/en-us/power-bi/visuals/power-bi-visualization-filled-maps-choropleths) | Allow people in your org to use the map and filled map visualizations in their reports. |
| [Integration with SharePoint and Microsoft Lists](service-admin-portal-integration#integration-with-sharepoint-and-microsoft-lists) | Users in the organization can launch Power BI from SharePoint lists and Microsoft Lists. Then they can build Power BI reports on the data in those lists and publish them back to the lists. |
| [Dremio SSO](https://powerquery.microsoft.com/blog/azure-ad-based-single-sign-on-for-dremio-cloud-and-power-bi) | Enable SSO capability for Dremio. By enabling, user access token information, including name and email, will be sent to Dremio for authentication. |
| [Snowflake SSO](/en-us/power-bi/connect-data/service-connect-snowflake) | Enable SSO capability for Snowflake. By enabling, user access token information, including name and email, will be sent to Snowflake for authentication. [Learn More](https://aka.ms/snowflakesso) |
| [Redshift SSO](/en-us/power-bi/connect-data/service-gateway-sso-overview) | Enable SSO capability for Redshift. By enabling, user access token information, including name and email, will be sent to Redshift for authentication. |
| [Google BigQuery SSO](/en-us/power-query/connectors/google-bigquery-aad) | Enable SSO capability for Google BigQuery. By enabling, user access token information, including name and email, will be sent to Google BigQuery for authentication. |
| [Enable Denodo SSO](/en-us/power-query/connectors/denodo) | Enable SSO capability for Denodo. By enabling, user access token information, including name and email, will be sent to Denodo for authentication. |
| [Enable Starburst SSO](/en-us/power-query/connectors/starburst) | Enable SSO capability for Starburst. By enabling, user access token information, including name and email, will be sent to Starburst for authentication. |
| Microsoft Entra single sign-on for data gateway | Users can use Microsoft Entra single sign-on (SSO) to authenticate to on-premises / virtual network data gateways and access data sources.With this setting on, user access token information, including names and emails, is sent to data sources to authenticate to the on-premises / virtual network data gateway service. [Learn More](https://go.microsoft.com/fwlink/?linkid=2256374) |
| Microsoft Entra single sign-on for virtual network (VNet) data gateway | Users can use Microsoft Entra single sign-on (SSO) to authenticate to virtual network (VNet) data gateways and access data sources.With this setting on, user access token information, including names and emails, is sent to data sources to authenticate to the virtual network data gateway service. [Learn More](https://go.microsoft.com/fwlink/?linkid=2256374) |
| [Users can view Power BI files saved in OneDrive and SharePoint (preview)](/en-us/power-bi/collaborate-share/service-sharepoint-viewer) | Users in the organization can view Power BI files saved in OneDrive for Business or SharePoint document libraries. The permissions to save and share Power BI files in OneDrive and SharePoint document libraries are controlled by permissions managed in OneDrive and SharePoint. [Learn More](https://go.microsoft.com/fwlink/?linkid=2224280) |
| [Users can share links to Power BI files stored in OneDrive and SharePoint through Power BI Desktop (preview)](service-admin-portal-integration#users-can-share-links-to-power-bi-files-stored-in-onedrive-and-sharepoint-through-power-bi-desktop-preview) | Users who have saved Power BI files (.pbix) to OneDrive and SharePoint can share links to those files using Power BI Desktop. [Learn More](https://go.microsoft.com/fwlink/?linkid=2227462) |
| Enable granular access control for all data connections | Enforce strict access control for all data connection types. When this is turned on, shared items will be disconnected from data sources if they're edited by users who don't have permission to use the data connections. [Learn More](https://go.microsoft.com/fwlink/?linkid=2226159) |
| [Semantic models can export data to OneLake](/en-us/power-bi/enterprise/onelake-integration-overview) | Semantic models configured for OneLake integration can send import tables to OneLake. Once the data is in OneLake, users can include the exported tables in Fabric items, including lakehouses and warehouses. [Learn More](https://go.microsoft.com/fwlink/?linkid=2252165) |
| [Semantic model owners can choose to automatically update semantic models from files imported from OneDrive or SharePoint](/en-us/power-bi/connect-data/refresh-desktop-file-onedrive) | Semantic model owners can choose to allow semantic models to be automatically updated with changes made to the corresponding Power BI files (.pbix) stored in OneDrive or SharePoint. File changes can include new and modified data connections.Turn off this setting to prevent automatic updates to semantic models. [Learn More](https://go.microsoft.com/fwlink/?linkid=2240760) |
| ArcGIS GeoAnalytics for Fabric Runtime | Users in your organization can use Esri's ArcGIS GeoAnalytics for Fabric Runtime in Microsoft's Fabric Spark Runtime. ArcGIS GeoAnalytics delivers spatial analysis to your big data by extending Apache Spark with ready-to-use spatial SQL functions and analysis tools. [Learn More](https://go.microsoft.com/fwlink/?linkid=2281344) |
| Allow non-Entra ID auth in Eventstream | Users can enhance the security of data streaming by disabling key-based authentication in Eventstream's Custom Endpoint, ensuring that only Microsoft Entra ID (formerly Azure Active Directory) authentication is allowed. This reduces the risk of unauthorized access to Fabric Eventstream through non-Entra ID authentication methods. |
| Users can connect to data sources by using Apache Arrow database connectivity (ADBC) | ADBC offers enhanced performance, security, and reliability compared to legacy connectivity options. When enabled, data connections to supported data sources use ADBC by default. [Learn More](https://aka.ms/ADBCOverview) |

## [Power BI visuals](/en-us/power-bi/admin/organizational-visuals)

| Setting name | Description |
| --- | --- |
| [Allow visuals created using the Power BI SDK](/en-us/power-bi/admin/organizational-visuals#visuals-from-appsource-or-a-file) | Users in the organization can add, view, share, and interact with visuals imported from AppSource or from a file. Visuals allowed in the "Organizational visuals" page are not affected by this setting. [Learn More](https://go.microsoft.com/fwlink/?linkid=2121300) |
| [Add and use certified visuals only (block uncertified)](/en-us/power-bi/admin/organizational-visuals#certified-power-bi-visuals) | Users in the organization with permissions to add and use visuals can add and use certified visuals only. Visuals allowed in the "Organizational visuals" page are not affected by this setting, regardless of certification. [Learn More](https://go.microsoft.com/fwlink/?linkid=2036068) |
| [Allow downloads from custom visuals](/en-us/power-bi/admin/organizational-visuals#export-data-to-file) | Enabling this setting will let custom visuals download any information available to the visual (such as summarized data and visual configuration) upon user consent. It is not affected by download restrictions applied in your organization's Export and sharing settings. [Learn More](https://go.microsoft.com/fwlink/?linkid=2185233&amp;clcid=0x409) |
| [AppSource Custom Visuals SSO](organizational-visuals#appsource-custom-visuals-sso) | Enable SSO capability for AppSource custom visuals. This feature allows custom visuals from AppSource to get Microsoft Entra ID access tokens for signed-in users through the Authentication API. Microsoft Entra ID access tokens include personal information, including users' names and email addresses, and may be sent across regions and compliance boundaries. [Learn More](https://go.microsoft.com/fwlink/?linkid=2236555) |
| [Allow access to the browser's local storage](organizational-visuals#appsource-custom-visuals-sso) | When this setting is on, custom visuals can store information on the user's browser's local storage. [Learn More](https://go.microsoft.com/fwlink/?linkid=2195648) |

## [R and Python visuals settings](service-admin-portal-r-python-visuals)

| Setting name | Description |
| --- | --- |
| [Interact with and share R and Python visuals](service-admin-portal-r-python-visuals#interact-with-and-share-r-and-python-visuals) | Users in the organization can interact with and share visuals created with R or Python scripts. |

## [Audit and usage settings](service-admin-portal-audit-usage)

| Setting name | Description |
| --- | --- |
| [Usage metrics for content creators](/en-us/power-bi/collaborate-share/service-modern-usage-metrics) | Users in the organization can see usage metrics for dashboards, reports and semantic models that they have appropriate permissions to. [Learn More](https://go.microsoft.com/fwlink/?linkid=2352692) |
| [Per-user data in usage metrics for content creators](/en-us/power-bi/collaborate-share/service-modern-usage-metrics#exclude-user-information-from-usage-metrics-reports) | Usage metrics for content creators will expose display names and email addresses of users who are accessing content. |
| [Show user data in the Fabric Capacity Metrics app and reports](/en-us/fabric/enterprise/metrics-app) | With this setting on, active user data, including names and email addresses, are displayed in the Capacity Metrics app and reports. [Learn More](https://go.microsoft.com/fwlink/?linkid=2281580) |
| [Azure Log Analytics connections for workspace administrators](/en-us/power-bi/transform-model/log-analytics/desktop-log-analytics-configure) | NO DESCRIPTION IN UI |
| [Workspace admins can turn on monitoring for their workspaces (preview)](../fundamentals/enable-workspace-monitoring) | Workspace admins can turn on monitoring for their workspaces. When a workspace admin turns on monitoring, a read-only Eventhouse that includes a KQL database is created. After the Eventhouse and KQL database are added to the workspace, logging is turned on and data is sent to the database. [Learn More](https://go.microsoft.com/fwlink/?linkid=2245715) |
| [Microsoft can store query text to aid in support investigations](query-text-storage) | Query text for some items, including semantic models, is securely stored for usage during support investigations. Turn off this setting to stop the service from storing query text.Turning off this setting might negatively impact Microsoft's ability to provide support for the Fabric service. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262696) |

## [Dashboard settings](service-admin-portal-dashboard)

| Setting name | Description |
| --- | --- |
| [Web content on dashboard tiles](/en-us/power-bi/create-reports/service-dashboard-add-widget#add-web-content) | Users in the organization can add and view web content tiles on Power BI dashboards. Note: This may expose your org to security risks via malicious web content. |

## [Developer settings](service-admin-portal-developer)

| Setting name | Description |
| --- | --- |
| [Embed content in apps](/en-us/power-bi/developer/embedded/embedded-analytics-power-bi) | Users in the organization can embed Power BI dashboards and reports in Web applications using "Embed for your customers" method. [Learn More](https://go.microsoft.com/fwlink/?linkid=2141877) |
| [Service principals can create workspaces, connections, and deployment pipelines](service-admin-portal-developer#service-principals-can-create-workspaces-connections-and-deployment-pipelines) | This setting allows service principals to create workspaces, connections, and deployment pipelines. To allow service principals to call the rest of Fabric public APIs, turn on the setting titled "Service principals can call Fabric public APIs". [Learn More](https://go.microsoft.com/fwlink/?linkid=2301112) |
| [Service principals can call Fabric public APIs](service-admin-portal-developer#service-principals-can-call-fabric-public-apis) | This setting allows service principals with the appropriate [roles and item permissions](https://go.microsoft.com/fwlink/?linkid=2301209) to call Fabric public APIs. To allow service principals to create workspaces, connections, and deployment pipelines turn on the setting titled "Service principals can create workspaces, connections, and deployment pipelines". [Learn More](https://go.microsoft.com/fwlink/?linkid=2301309) |
| [Allow service principals to create and use profiles](/en-us/power-bi/developer/embedded/embed-multi-tenancy) | Allow service principals in your organization to create and use profiles. |
| [Block ResourceKey Authentication](service-admin-portal-developer#block-resourcekey-authentication) | For extra security, block using resource key based authentication. This means users not allowed to use streaming semantic models API using resource key. |
| [Define maximum number of Fabric identities in a tenant](service-admin-portal-developer#define-maximum-number-of-fabric-identities-in-a-tenant) | Allow admins to specify the maximum number of Fabric identities that can be created in a tenant. If this setting is disabled, up to 10000 Fabric identities can be created in a tenant. [Learn more](https://go.microsoft.com/fwlink/?linkid=2339322). |

## [Admin API settings](service-admin-portal-admin-api-settings)

| Setting name | Description |
| --- | --- |
| [Service principals can access read-only admin APIs](/en-us/power-bi/enterprise/read-only-apis-service-principal-authentication) | Web apps registered in Microsoft Entra ID can use service principals, rather than user credentials, to authenticate to read-only admin APIs.To allow an app to use a service principal as an authentication method, the service principal must be added to an allowed security group. Service principals included in allowed security groups will have read-only access to all the information available through admin APIs, which can include users' names and emails, and detailed metadata about semantic models and reports. [Learn More](https://go.microsoft.com/fwlink/?linkid=2110545) |
| [Service principals can access admin APIs used for updates](/en-us/fabric/admin/enable-service-principal-admin-apis) | Web apps registered in Microsoft Entra ID can use service principals, rather than user credentials, to authenticate to admin APIs used for updates.To allow an app to use a service principal as an authentication method, add the service principal to an allowed security group. Service principals in allowed security groups have full access to the information available through admin APIs, including users' names and emails, and detailed metadata about items. [Learn More](https://go.microsoft.com/fwlink/?linkid=2263376) |
| [Enhance admin APIs responses with detailed metadata](service-admin-portal-admin-api-settings#enhance-admin-apis-responses-with-detailed-metadata) | Users and service principals allowed to call Power BI admin APIs may get detailed metadata about Power BI items. For example, responses from GetScanResult APIs will contain the names of semantic model tables and columns. [Learn More](https://go.microsoft.com/fwlink/?linkid=2153790)Note: For this setting to apply to service principals, make sure the tenant setting allowing service principals to use read-only admin APIs is enabled. [Learn More](https://go.microsoft.com/fwlink/?linkid=2110545) |
| [Enhance admin APIs responses with DAX and mashup expressions](service-admin-portal-admin-api-settings#enhance-admin-apis-responses-with-dax-and-mashup-expressions) | Users and service principals eligible to call Power BI admin APIs will get detailed metadata about queries and expressions comprising Power BI items. For example, responses from GetScanResult API will contain DAX and mashup expressions. [Learn More](https://go.microsoft.com/fwlink/?linkid=2153889)Note: For this setting to apply to service principals, make sure the tenant setting allowing service principals to use read-only admin APIs is enabled. [Learn More](https://go.microsoft.com/fwlink/?linkid=2110545) |

## [Gen1 dataflow settings](service-admin-portal-dataflow)

| Setting name | Description |
| --- | --- |
| [Create and use Gen1 dataflows](/en-us/power-bi/transform-model/dataflows/dataflows-introduction-self-service) | Users in the organization can create and use Gen1 dataflows. [Learn More](https://go.microsoft.com/fwlink/?linkid=869491) |

## [Template app settings](service-admin-portal-template-app)

| Setting name | Description |
| --- | --- |
| [Publish template apps](/en-us/power-bi/connect-data/service-template-apps-overview) | Users in the organization can publish template apps for distribution to clients outside of the organization. [Learn More](https://go.microsoft.com/fwlink/?linkid=2091856). |
| [Install template apps](service-admin-portal-template-app#install-template-apps) | Users in the organization can install template apps created outside the organization. When a template app is installed, an upgraded workspace is created. [Learn More](https://go.microsoft.com/fwlink/?linkid=2091014) |
| Install template apps not listed in AppSource | Users in the organization who have been granted permission to install template apps which were not published to Microsoft AppSource. [Learn More](https://go.microsoft.com/fwlink/?linkid=2091200). |

## Q&A settings

| Setting name | Description |
| --- | --- |
| Review questions | Allow semantic model owners to review questions people asked about their data. |
| [Synonym sharing](/en-us/power-bi/natural-language/q-and-a-tooling-intro#field-synonyms) | Allow people to share Q&A synonyms with your organization. [Learn More](https://go.microsoft.com/fwlink/?linkid=2163449) |

## Explore settings (preview)

| Setting name | Description |
| --- | --- |
| [Users with view permission can launch Explore](/en-us/power-bi/consumer/explore-data-service#permissions-and-requirements) | Explore is a light-weight visual data exploration experience that enables people to quickly and easily do ad hoc analysis. This setting allows people with view permission on a semantic model to launch Explore from that model and from items connected to it. [Learn More](https://go.microsoft.com/fwlink/?linkid=2305305) |

## Semantic Model Security

| Setting name | Description |
| --- | --- |
| [Block republish and disable package refresh](service-admin-portal-dataset-security#block-republish-and-disable-package-refresh) | Disable package refresh, and only allow the semantic model owner to publish updates. |

## [Advanced networking](service-admin-portal-advanced-networking)

| Setting name | Description |
| --- | --- |
| [Tenant-level Private Link](../security/security-private-links-overview) | Increase security by allowing people to use a Private Link to access your Fabric tenant. Someone will need to finish the set-up process in Azure. If that's not you, grant permission to the right person or group by entering their email. [Learn More](https://aka.ms/PrivateLinksLearnMore)[Set-up instructions](https://aka.ms/PrivateLinksSetupInstructions)Review the [considerations and limitations](https://aka.ms/PrivateLinksConsiderationsAndLimitations) section before enabling private endpoints. |
| [Block Public Internet Access](/en-us/power-bi/enterprise/service-security-private-links) | For extra security, block access to your Fabric tenant via the public internet. This means people who don't have access to the Private Link won't be able to get in. Keep in mind, turning this on could take 10 to 20 minutes to take effect. [Learn More](https://aka.ms/PrivateLinksLearnMore)[Set-up instructions](https://aka.ms/PrivateLinksSetupInstructions) |
| Configure workspace-level inbound network rules | With this setting on, workspace admins can configure inbound private link access protection in workspace settings. When a workspace is configured to restrict inbound network access, existing tenant-level private links can no longer connect to these workspaces. Turning off this setting reverts all workspaces to their previous configuration. [Learn More](https://go.microsoft.com/fwlink/?linkid=2272575) |
| Configure workspace-level outbound network rules | With this setting on, workspace admins can configure outbound access protection in workspace settings. Turning off this tenant setting also turns off outbound access protection in all the workspaces in the tenant. [Learn More](https://go.microsoft.com/fwlink/?linkid=2310620) |
| Configure workspace-level IP firewall rules and trusted resource instances | With this setting enabled, workspace admins can configure IP firewall rules and trusted resource instances in Workspace settings. Even when the tenant blocks public access, this setting still allows workspace admins to grant access from specific IP firewall rules and designated trusted resource instances. [Learn More](https://go.microsoft.com/fwlink/?linkid=2223103) |

## Encryption

| Setting name | Description |
| --- | --- |
| Apply customer-managed keys | With this setting turned on, users can configure workspace level encryption using customer-managed keys to protect their data. When turned off, the default is to use Microsoft managed keys. [Learn More](https://go.microsoft.com/fwlink/?linkid=2308801) |

## Scorecards settings

| Setting name | Description |
| --- | --- |
| Create and use Scorecards | Users in the organization can create and use Scorecards |

## [User experience experiments](service-admin-portal-user-experience-experiments)

| Setting name | Description |
| --- | --- |
| Help Power BI optimize your experience | Users in this organization will get minor user experience variations that the Power BI team is experimenting with, including content, layout, and design, before they go live for all users. |

## [Share data with your Microsoft 365 services](admin-share-power-bi-metadata-microsoft-365-services)

| Setting name | Description |
| --- | --- |
| [Share Fabric data with your Microsoft 365 services](admin-share-power-bi-metadata-microsoft-365-services) | When this setting is enabled, Microsoft Fabric data can be stored and displayed in Microsoft 365 services. Fabric data (including Power BI report titles, chart axis labels, Fabric data agent instructions, or open and sharing history) may be used to improve Microsoft 365 services like search results and recommended content lists. [Learn More](https://go.microsoft.com/fwlink/?linkid=2202379)Users can browse or get recommendations only for content they have access to. Users will see metadata about Fabric items (including refresh dates and workspace names in search listings) and see item content (like chart axis labels or titles reflected in Copilot summarizations) to enhance Microsoft 365 services.This setting is automatically enabled only if your Microsoft Fabric and Microsoft 365 tenants are in the same geographical region. You may disable this setting. [Where is my Microsoft Fabric tenant located?](https://go.microsoft.com/fwlink/?linkid=2237979) |

## [Insights settings](service-admin-portal-insights)

| Setting name | Description |
| --- | --- |
| [Receive notifications for top insights (preview)](/en-us/power-bi/create-reports/insights) | Users in the organization can enable notifications for top insights in report settings |
| Show entry points for insights (preview) | Users in the organization can use entry points for requesting insights inside reports |

## [Datamart settings](service-admin-portal-datamart)

| Setting name | Description |
| --- | --- |
| [Create Datamarts (preview)](/en-us/power-bi/transform-model/datamarts/datamarts-administration) | Users in the organization can create Datamarts |

## Semantic model settings

| Setting name | Description |
| --- | --- |
| Users can edit semantic models in the Power BI service | Turn on this setting to allow users to edit semantic models in the service. This setting doesn't apply to DirectLake semantic models or editing a semantic model through an API or XMLA endpoint. [Learn More](https://go.microsoft.com/fwlink/?linkid=2227332) |

## Kusto settings

| Setting name | Description |
| --- | --- |
| Allow Real-Time Dashboards to include embedded app visuals | Enabling this setting allows dashboards to accept embedded app visuals, which are external apps embedded as iframes within Real-Time Dashboards |

## [Scale-out settings](service-admin-portal-scale-out)

| Setting name | Description |
| --- | --- |
| [Scale out queries for large semantic models](/en-us/power-bi/enterprise/service-premium-scale-out) | For semantic models that use the large semantic model storage format, Power BI Premium can automatically distribute queries across additional semantic model replicas when query volume is high. |

## [OneLake settings](service-admin-portal-onelake)

| Setting name | Description |
| --- | --- |
| [Users can access data stored in OneLake with apps external to Fabric](../onelake/security/fabric-onelake-security#allow-apps-running-outside-of-fabric-to-access-data-via-onelake) | Users can access data stored in OneLake with apps external to the Fabric environment, such as custom applications created with Azure Data Lake Storage (ADLS) APIs, OneLake File Explorer, and Databricks. Users can already access data stored in OneLake with apps internal to the Fabric environment, such as Spark, Data Engineering, and Data Warehouse. [Learn More](https://go.microsoft.com/fwlink/?linkid=2231198) |
| Use short-lived user-delegated SAS tokens | OneLake SAS tokens enable applications to access data in OneLake through short-lived SAS tokens, based on a Microsoft Fabric user's Entra identity. These token's permissions can be further limited to provide least privileged access and cannot exceed a lifetime of one hour. [Learn More](https://go.microsoft.com/fwlink/?linkid=2268260) |
| Authenticate with OneLake user-delegated SAS tokens | Allow applications to authenticate using a OneLake SAS token. Fabric users can create OneLake SAS by requesting a user delegation key. The tenant setting, Use short-lived user delegated SAS tokens, must be turned on to generate user delegation keys. The lifetimes of the user delegation keys and SAS tokens cannot exceed one hour. [Learn More](https://go.microsoft.com/fwlink/?linkid=2268260) |
| [Users can sync data in OneLake with the OneLake File Explorer app](../onelake/onelake-file-explorer) | Turn on this setting to allow users to use OneLake File Explorer. This app will sync OneLake items to Windows File Explorer, similar to OneDrive. [Learn More](https://go.microsoft.com/fwlink/?linkid=2231052) |
| Include end-user identifiers in OneLake diagnostic logs | Control whether OneLake diagnostic logs capture end user identifiable information (EUII), such as email addresses and IP addresses. When enabled, these fields are recorded to support diagnostics, investigations, and usage analysis across your tenant. When disabled, these fields are redacted from new events. [Learn More](https://go.microsoft.com/fwlink/?linkid=2335502) |

## [Git integration](git-integration-admin-settings)

| Setting name | Description |
| --- | --- |
| [Users can synchronize workspace items with their Git repositories](../cicd/git-integration/intro-to-git-integration) | Users can import and export workspace items to Git repositories for collaboration and version control. Turn off this setting to prevent users from syncing workspace items with their Git repositories. [Learn More](https://go.microsoft.com/fwlink/?linkid=2240844) |
| [Users can export items to Git repositories in other geographical locations](git-integration-admin-settings#users-can-export-items-to-git-repositories-in-other-geographical-locations) | The workspace and the Git repository may reside in different geographies. Turn on this setting to allow users to export items to Git repositories in other geographies. |
| [Users can export workspace items with applied sensitivity labels to Git repositories](git-integration-admin-settings#users-can-export-workspace-items-with-applied-sensitivity-labels-to-git-repositories) | Turn on this setting to allow users to export items with applied sensitivity labels to their Git repositories. |
| [Users can sync workspace items with GitHub repositories](git-integration-admin-settings#users-can-sync-workspace-items-with-github-repositories) | Users can select GitHub as their Git provider and sync items in their workspaces with GitHub repositories. |

## [Copilot and Azure OpenAI Service](service-admin-portal-copilot)

| Setting name | Description |
| --- | --- |
| Users can use Copilot and other features powered by Azure OpenAI | When this setting is on, users can access Fabric features powered by Azure OpenAI, including Copilot and Fabric AI agents. Check documentation for the most recent [list of these features](https://aka.ms/fabric/copilot-ai-feature-status). This setting can be managed at both the tenant and the capacity levels. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262239)For customers in the EU Data Boundary, this setting adheres to Microsoft Fabric's EU Data Boundary commitments. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262243)By turning on this setting, you agree to the [Preview Terms](https://go.microsoft.com/fwlink/?linkid=2262241) for any [AI features in preview](https://aka.ms/fabric/copilot-ai-feature-status). |
| Users can access a standalone, cross-item Power BI Copilot experience (preview) | When this setting is turned on, users will be able to access a Copilot experience that allows them to find, analyze, and discuss different Fabric items in a dedicated tab available via the Power BI navigation pane. This setting requires the following tenant setting to be enabled: "Users can use Copilot and other features powered by Azure OpenAI." [Learn More](https://go.microsoft.com/fwlink/?linkid=2306434) |
| [Data sent to Azure OpenAI can be processed outside your capacity's geographic region, compliance boundary, or national cloud instance](/en-us/azure/azure-maps/geographic-scope) | This setting is only applicable for customers who want to use Copilot and AI features in Fabric powered by Azure OpenAI, and whose capacity's geographic region is outside of the EU Data Boundary or the United States. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262085)When this setting is on, data sent to Copilot and other generative AI features can be processed outside your capacity's geographic region, compliance boundary, or national cloud instance. Check [documentation](https://aka.ms/fabric-copilot-overview-data) for the types of data this might include. This setting can be managed at both the tenant and the capacity levels. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262242)By turning on this setting, you agree to the [Preview Terms](https://go.microsoft.com/fwlink/?linkid=2262241) for any [AI features in preview](https://aka.ms/fabric/copilot-ai-feature-status). |
| [Capacities can be designated as Fabric Copilot capacities](service-admin-portal-copilot#fabric-copilot-capacities) | With this setting on, capacity admins can designate capacities as Fabric Copilot capacities. Copilot capacities are special capacity types that allow your organization to consolidate users' Copilot usage and billing on a single capacity. [Learn More](https://aka.ms/fcctenant)When users use Copilot features, capacity admins can see the names of the items associated with users' Copilot activity. [Learn More](https://aka.ms/fccusage) |
| [Data sent to Azure OpenAI can be stored outside your capacity's geographic region, compliance boundary, or national cloud instance](/en-us/fabric/fundamentals/copilot-fabric-overview#available-regions) | This setting is only applicable for customers who want to use Copilot and AI features in Fabric powered by Azure OpenAI, and whose capacity's geographic region is outside of the EU Data Boundary or the United States. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262085)When this setting is turned on, data sent to Azure OpenAI can be stored outside your capacity's geographic region, compliance boundary, or national cloud instance. Check [documentation](https://aka.ms/fabric-copilot-overview-data) for the types of experiences and data this might include. [Learn More](https://go.microsoft.com/fwlink/?linkid=2262242)By turning on this setting, you agree to the [Preview Terms](https://go.microsoft.com/fwlink/?linkid=2262241) for any [AI features in preview](https://aka.ms/fabric/copilot-ai-feature-status). |
| Only show approved items in the standalone Copilot in Power BI experience (preview) | When this is turned on, only apps, data agents, and items marked as "approved for Copilot" will be shown in standalone Copilot. Users will still be able to manually attach items to ask questions. Copilot item usage is always subject to user permissions. [Learn More](https://go.microsoft.com/fwlink/?linkid=2311781) |

## Azure Maps services

| Setting name | Description |
| --- | --- |
| Users can use Azure Maps services | When this setting is enabled, users can access the features powered by Azure Maps services. [Learn More](https://go.microsoft.com/fwlink/?linkid=2310613)For customers in the EU Data Boundary, this setting adheres to Microsoft Fabric's EU Data Boundary commitments. [Learn More](https://go.microsoft.com/fwlink/?linkid=2310806)Use of Azure Maps is subject to the following [Terms of use](https://go.microsoft.com/fwlink/?linkid=2271924). |
| Data sent to Azure Maps can be processed outside your capacity's geographic region, compliance boundary or national cloud instance | Azure Maps services are currently not available in all regions and geographies. With this setting on, data sent to Azure Maps can be processed in a region where the service is available, which might be outside your capacity's geographic region, compliance boundary, or national cloud instance. [Learn More](https://go.microsoft.com/fwlink/?linkid=2289253) |
| Users can use Azure Maps Weather Services (Preview) | When this setting is enabled, users can access weather data from Azure Maps Weather, sourced from AccuWeather [Learn More](https://go.microsoft.com/fwlink/?linkid=2340279) |

## Additional workloads

| Setting name | Description |
| --- | --- |
| Workspace admins can add and remove additional workloads (preview) | Workspace admins can add and remove workloads in their workspaces. If this setting is turned off, any existing workloads will stay added and items created with those workloads continue to work normally.When users interact with a workload, their data and access tokens, including name and email, are sent to the publisher. Sensitivity labels and protection settings including encryption aren't applied to items created with workloads. [Learn More](https://go.microsoft.com/fwlink/?linkid=2268082) |
| [Capacity admins and contributors can add and remove additional workloads](../workload-development-kit/environment-setup#enable-the-development-tenant-setting) | Capacity admins or individuals granted Contributor permission in Capacity settings can add and remove additional workloads in capacities. If this setting is turned off, any existing workloads will stay added and items created with those workloads continue to work normally.When users interact with a workload, their data and access tokens, including name and email, are sent to the publisher. Sensitivity labels and protection settings including encryption aren't applied to items created with workloads. [Learn More](https://go.microsoft.com/fwlink/?linkid=2268082) |
| [Workspace admins can develop partner workloads](/en-us/fabric/workload-development-kit/environment-setup#enable-the-development-tenant-setting) | Workspace admins can develop partner workloads with a local machine development environment. Turning off this feature will prevent developers from uploading to this workspace. [Learn More](https://go.microsoft.com/fwlink/?linkid=2268434) |
| [Users can see and work with additional workloads not validated by Microsoft](../workload-development-kit/publish-workload-requirements) | Turn on this setting to allow users to see and work with additional workloads not validated by Microsoft. Make sure that you only add workloads from publishers that you trust to meet your organization's policies. [Learn More](https://go.microsoft.com/fwlink/?linkid=2282060) |

## Azure AI Service

| Setting name | Description |
| --- | --- |
| Allow data processing outside geographic region for Azure AI services | This setting applies only to customers using prebuilt AI services in Fabric whose capacity is in a region not covered by Azure AI services. [Learn More](https://aka.ms/Fabric/AI-Serivce/Regions)When enabled, data sent to Azure AI services may be processed outside your capacity's geographic region, compliance boundary, or national cloud instance. Check the [documentation](https://aka.ms/Fabric/AI-Service/Data) for details about what data types are affected. You can manage this setting at both tenant and capacity levels. [Learn More](https://aka.ms/Fabric/AI-Service/Tenant-Setting) |