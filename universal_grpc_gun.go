// Copyright (c) 2017 Yandex LLC. All rights reserved.
// Use of this source code is governed by a MPL 2.0
// license that can be found in the LICENSE file.
// Author: Vladimir Skipor <skipor@yandex-team.ru>

package main

import (
	"context"
	"encoding/json"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/spf13/afero"
	"github.com/yandex/pandora/cli"
	"github.com/yandex/pandora/core"
	"github.com/yandex/pandora/core/aggregator/netsample"
	"github.com/yandex/pandora/core/import"
	"github.com/yandex/pandora/core/register"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"log"
)

type Ammo struct {
	Tag      string                 `json:"tag"`
	Call     string                 `json:"call"`
	Metadata map[string]string      `json:"metadata"`
	Payload  map[string]interface{} `json:"payload"`
}

type Sample struct {
	URL              string
	ShootTimeSeconds float64
}

type GunConfig struct {
	Target string `validate:"required"` // Configuration will fail, without target defined
}

type Gun struct {
	// Configured on construction.
	client grpc.ClientConn
	conf   GunConfig
	// Configured on Bind, before shooting
	aggr core.Aggregator // May be your custom Aggregator.
	core.GunDeps
	stub     grpcdynamic.Stub
	services map[string]desc.MethodDescriptor
}

func NewGun(conf GunConfig) *Gun {
	return &Gun{conf: conf}
}

func (g *Gun) Bind(aggr core.Aggregator, deps core.GunDeps) error {
	conn, err := grpc.Dial(
		g.conf.Target,
		grpc.WithInsecure(),
		grpc.WithUserAgent("pandora load test"))
	if err != nil {
		log.Fatalf("FATAL: %s", err)
	}
	g.client = *conn
	g.aggr = aggr
	g.GunDeps = deps
	g.stub = grpcdynamic.NewStub(conn)

	meta := make(metadata.MD)
	refCtx := metadata.NewOutgoingContext(context.Background(), meta)
	refClient := grpcreflect.NewClient(refCtx, reflectpb.NewServerReflectionClient(conn))
	listServices, err := refClient.ListServices()
	g.services = make(map[string]desc.MethodDescriptor)
	for _, s := range listServices {
		service, err := refClient.ResolveService(s)
		if err != nil {
			log.Fatalf("FATAL: %s", err)
		}
		listMethods := service.GetMethods()
		for _, m := range listMethods {
			log.Printf("Method: %s", m.GetFullyQualifiedName())
			g.services[m.GetFullyQualifiedName()] = *m
		}
	}
	return nil
}

func (g *Gun) Shoot(ammo core.Ammo) {
	customAmmo := ammo.(*Ammo) // Shoot will panic on unexpected ammo type. Panic cancels shooting.
	g.shoot(customAmmo)
}

func (g *Gun) shoot(ammo *Ammo) {
	code := 0
	sample := netsample.Acquire(ammo.Tag)
	defer func() {
		sample.SetProtoCode(code)
		g.aggr.Report(sample)
	}()

	method, ok := g.services[ammo.Call]
	if ok != true {
		log.Printf("No such method: %s", ammo.Call)
		return
	}
	log.Printf("MethodDescriptor: %s", method.GetInputType())
	payloadJson, err := json.Marshal(ammo.Payload)
	if err != nil {
		log.Fatalf("FATAL: %s", err)
		return
	}
	md := method.GetInputType()
	message := dynamic.NewMessage(md)
	err = message.UnmarshalJSON(payloadJson)
	if err != nil {
		code = 400
		log.Printf("BAD REQUEST: %s", err)
		return
	}

	// MD is actually a map from string to a list of strings
	meta := make(metadata.MD)
	if ammo.Metadata != nil && len(ammo.Metadata) > 0 {
		for key, value := range ammo.Metadata {
			meta = metadata.Pairs(key, value)
		}
	}

	ctx := metadata.NewOutgoingContext(context.Background(), meta)
	out, err := g.stub.InvokeRpc(ctx, &method, message)
	if err != nil {
		code = 0
		log.Printf("BAD REQUEST: %s", err)
	}
	if out != nil {
		code = 200
	}
}

func main() {
	// Standard imports.
	fs := afero.NewOsFs()
	coreimport.Import(fs)

	// Custom imports. Integrate your custom types into configuration system.
	coreimport.RegisterCustomJSONProvider("custom_provider", func() core.Ammo { return &Ammo{} })

	register.Gun("universal_grpc_gun", NewGun, func() GunConfig {
		return GunConfig{
			Target: "default target",
		}
	})

	cli.Run()
}
