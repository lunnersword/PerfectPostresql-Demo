//
//  main.swift
//  PerfectTemplate
//
//  Created by Kyle Jessup on 2015-11-05.
//	Copyright (C) 2015 PerfectlySoft, Inc.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2015 - 2016 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//
import PerfectHTTPServer

do {
    let p = PostgresConnector()
    p.createCity()
    p.createWeather()
    p.insertIntoWeather(city: "San Francisco", temp_lo: 46, temp_hi: 50, prcp: 0.25, date: "1994-11-27")
    p.insertIntoWeather(city: "San Francisco", temp_lo: 43, temp_hi: 57, prcp: 0.0, date: "1994-11-29")
    p.insertIntoCity(name: "San Francisco", location: "(-194.0, 53.0)")
    p.insertIntoWeather(city: "Hayward", temp_lo: 37, temp_hi: 54, prcp: 0.0, date: "1994-11-29")
    p.selectFromWeather()
    p.select1()
    p.select2()
    p.select3()
    p.join1()
    p.leftOuterJoin()
    p.rightOuterJoin()
    p.selfJoin()
    p.fullOuterJoin()
    p.aggregates()
    p.updateWeather()
    p.selectFromWeather()
    p.delete()
}

