---
title: Video testingddddddd
description: test page for several rules 
tags: [ products>sap-hana-cloud-platform, topic>cloud, topic>java]
primary_tag: tutorial:product/sapHana
---

## Prerequisites  
 - [End-to-End Weather App Scenario Part 8](http://go.sap.com/developer/tutorials/hcp-java-weatherapp-part8vbv454.html5645)

## Next Steps
 - [End-to-End Weather App Scenario Part 10](http://go.sap.com/developer/tutorials/hcp-java-weatherapp-part10.html)

## Details
Every month, a mebber of SCN is rsi

### Time to Complete
**15 min**

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]


[ACCORDION-BEGIN [STEP 1](Accordion component which contains Images in Body)]
    ![Repositories](Funny-Baby-11.jpg)
    
[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]

![Repositories](Funny-Baby-11.jpg)
    
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 2](Accordion component which contains link(s) in Body)]
Select a tutorial from the [Tutorial Navigator](http://go.sap.com/developer/tutorial-navigator.html) or the [Tutorial Catalog](http://go.sap.com/developer/tutorials.html)

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 3](Accordion component which contains tables in Body)]
***Tables:***

  **Example:** 

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]

First Header | Second Header
------------ | -------------
Content from cell 1 | Content from cell 2
Content in the first column | Content in the second column


and

| Left-Aligned  | Center Aligned  | Right Aligned |
| :------------ |:---------------:| -----:|
| col 3 is      | some wordy text | $1600 |
| col 2 is      | centered        |   $12 |
| zebra stripes | are neat        |    $1 |

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 4](Accordion component which contains markup code in Body)]

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]

```markup
    <?xml version="1.0" encoding="UTF-8"?>
    <web-app xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://java.sun.com/xml/ns/javaee" xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd" id="WebApp_ID" version="2.5">
    <display-name>cloud-weatherapp</display-name>
    <welcome-file-list>
    <welcome-file>index.html</welcome-file>
    </welcome-file-list>
    <servlet>
    <display-name>HelloWorldServlet</display-name>
    <servlet-name>HelloWorldServlet</servlet-name>
    <servlet-class>
        com.sap.hana.cloud.samples.weatherapp.web.HelloWorldServlet
    </servlet-class>
    </servlet>
    <servlet-mapping>
    <servlet-name>HelloWorldServlet</servlet-name>
    <url-pattern>/hello</url-pattern>
    </servlet-mapping>
    <login-config>
    <auth-method>FORM</auth-method>
    </login-config>
    <security-constraint>
    <web-resource-collection>
        <web-resource-name>Protected Area</web-resource-name>
        <url-pattern>/*</url-pattern>
    </web-resource-collection>
    <auth-constraint>
        <!-- Role Everyone will not be assignable -->
        <role-name>Everyone</role-name>
    </auth-constraint>
    <?xml version="1.0" encoding="UTF-8"?>
    <web-app xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://java.sun.com/xml/ns/javaee" xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd" id="WebApp_ID" version="2.5">
    <display-name>cloud-weatherapp</display-name>
    <welcome-file-list>
    <welcome-file>index.html</welcome-file>
    </welcome-file-list>
    <servlet>
    <display-name>HelloWorldServlet</display-name>
    <servlet-name>HelloWorldServlet</servlet-name>
    <servlet-class>
        com.sap.hana.cloud.samples.weatherapp.web.HelloWorldServlet
    </servlet-class>
    </servlet>
    <servlet-mapping>
    <servlet-name>HelloWorldServlet</servlet-name>
    <url-pattern>/hello</url-pattern>
    </servlet-mapping>
    <login-config>
    <auth-method>FORM</auth-method>
    </login-config>
    <security-constraint>
    <web-resource-collection>
        <web-resource-name>Protected Area</web-resource-name>
        <url-pattern>/*</url-pattern>
    </web-resource-collection>
    <auth-constraint>
        <!-- Role Everyone will not be assignable -->
        <role-name>Everyone</role-name>
    </auth-constraint>
    <?xml version="1.0" encoding="UTF-8"?>
    <web-app xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://java.sun.com/xml/ns/javaee" xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd" id="WebApp_ID" version="2.5">
    <display-name>cloud-weatherapp</display-name>
    <welcome-file-list>
    <welcome-file>index.html</welcome-file>
    </welcome-file-list>
    <servlet>
    <display-name>HelloWorldServlet</display-name>
    <servlet-name>HelloWorldServlet</servlet-name>
    <servlet-class>
        com.sap.hana.cloud.samples.weatherapp.web.HelloWorldServlet
    </servlet-class>
    </servlet>
    <servlet-mapping>
    <servlet-name>HelloWorldServlet</servlet-name>
    <url-pattern>/hello</url-pattern>
    </servlet-mapping>
    <login-config>
    <auth-method>FORM</auth-method>
    </login-config>
    <security-constraint>
    <web-resource-collection>
        <web-resource-name>Protected Area</web-resource-name>
        <url-pattern>/*</url-pattern>
    </web-resource-collection>
    <auth-constraint>
        <!-- Role Everyone will not be assignable -->
        <role-name>Everyone</role-name>
    </auth-constraint>
    <?xml version="1.0" encoding="UTF-8"?>
    <web-app xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns="http://java.sun.com/xml/ns/javaee" xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd" id="WebApp_ID" version="2.5">
    <display-name>cloud-weatherapp</display-name>
    <welcome-file-list>
    <welcome-file>index.html</welcome-file>
    </welcome-file-list>
    <servlet>
    <display-name>HelloWorldServlet</display-name>
    <servlet-name>HelloWorldServlet</servlet-name>
    <servlet-class>
        com.sap.hana.cloud.samples.weatherapp.web.HelloWorldServlet
    </servlet-class>
    </servlet>
    <servlet-mapping>
    <servlet-name>HelloWorldServlet</servlet-name>
    <url-pattern>/hello</url-pattern>
    </servlet-mapping>
    <login-config>
    <auth-method>FORM</auth-method>
    </login-config>
    <security-constraint>
    <web-resource-collection>
        <web-resource-name>Protected Area</web-resource-name>
        <url-pattern>/*</url-pattern>
    </web-resource-collection>
    <auth-constraint>
        <!-- Role Everyone will not be assignable -->
        <role-name>Everyone</role-name>
    </auth-constraint>
```

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 5](Accordion component which contains Text in Body)]
We are currently in open beta for the new SAP community. Here's your opportunity to test out the new features and give us your feedback. Not in the mood to try out the new community just yet? Then continue on to the SAP Community Network (SCN) where you currently collaborate with other members.

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]

We are currently in open beta for the new SAP community. Here's your opportunity to test out the new features and give us your feedback. Not in the mood to try out the new community just yet? Then continue on to the SAP Community Network (SCN) where you currently collaborate with other members.
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 6](Accordion component which contains Headers in Body)]
***Headers***

  **Example:** 

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
## This is an h2 header

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
### This is an h3 header

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
###### This is an h6 header
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 7](Accordion component which contains Lists in Body)]
***Lists***

  **Example:** 
  
Sometimes you want numbered lists:

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
1. One
2. Two 
3. Three

Sometimes you want bullet points:

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
* Start a line with a star
* Profit!
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 8](Accordion component which contains nested lists in Body)]
You can create nested lists: 

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
* item1
    * one_one
    * two
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 9](Accordion component which contains Blockquotes in Body)]
***Blockquotes***

  **Example:** 
In the words of Abraham Lincoln:

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
> Pardon my French
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 10](Accordion component which contains types of messages (Note, Caution and Warning) in Body)]
***There are three different types of messages: Note, Caution and Warning.***

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
>### Warning
>jhkjhkjhkjhkj
>>### Warning
>>>### Warning
>>>>### Warning
>>>>This is a Warning. 

&nbsp;

>### Caution
>iikjhiojhioji
>>### Caution
>>This is a Caution. 

&nbsp;

>### Note

>This is a note. 
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 11](Accordion component which contains Task Lists in Body)]
**Task Lists*** (Please note, this requires empty line before task list):

  **Example:** 
  
  [EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]  
- [x] @mentions, #refs, [links](), **formatting**, and ~~tags~~ supported
- [x] list syntax required (any unordered or ordered list supported)
- [x] this is a complete item
- [ ] this is an incomplete item
[ACCORDION-END]

[ACCORDION-BEGIN [STEP 12](Accordion component which contains code block and no code block in Body)]
***Code blocks:***

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]

```markup
 quit;
 !@#$%^&*&*(*(()_++|"}?><>??*&^%#!~~~~@33123-090=|"]?>{}|\\
  require 'redcarpet'
  markdown = Redcarpet.new("Hello World!")
  puts markdown.to_html
  exit;
```

```js
 quit;
 !@#$%^&*&*(*(()_++|"}?><>??*&^%#!~~~~@33123-090=|"]?>{}|\\
  require 'redcarpet'
  markdown = Redcarpet.new("Hello World!")
  puts markdown.to_html
  exit;
```

[EMBEDDED-VIDEO [](/content/dam/site/sapcom/multimedia/2016/08/8adbcaf9-807c-0010-82c7-eda71af511fa.mp4)]
[ACCORDION-END]
