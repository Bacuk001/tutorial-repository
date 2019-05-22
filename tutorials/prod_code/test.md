---
title: Jim's code syntax
description: 1code code
tags: [tutorial>beginner,products>sap-hana-cloud-platform, topic>cloud, topic>java]
primary_tag: tutorial:product/sapHana
---

  **Example:without code**
  ```swift
  // SAP CodeJam WWDC 2017 Playground

import UIKit

for repeatSAPCodeJamEveryHour in stride(from: 9, through: 18, by: 1) {
    learnAtCodeJam(miniEdition: true, WWDC: .AUX)
}

func learnAtCodeJam(miniEdition: Bool, WWDC: WWDC) {
    if (miniEdition && WWDC == .AUX) {
        learn(what: "SAP Cloud Platform")
        learn(what: "SAP iOS SDK")
        BuildApp.now()
    }
}


//
// What will you be doing on June 7?
//
func learn(what: String) {
    print("Now learning \(what)")
}

enum WWDC {
    case AUX
}

class BuildApp {
    static func now() {
        print("Building app")
    }
}
```
