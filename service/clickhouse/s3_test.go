package clickhouse_test

// func TestS3(t *testing.T) {
// 	sc, err := session.NewSession(&aws.Config{
// 		Credentials:      credentials.NewStaticCredentials("KZOqVTra982w51MK", "7Zsdaywu7i5C2AyvLkbupSyVlIzP8qJ0", ""),
// 		Endpoint:         aws.String("http://192.168.110.8:49000"),
// 		Region:           aws.String("zh-west-1"),
// 		DisableSSL:       aws.Bool(true),
// 		S3ForcePathStyle: aws.Bool(true), //virtual-host style方式，不要修改
// 	})
// 	assert.Nil(t, err)
// 	svc := s3.New(sc)

// 	//查看bucket
// 	results, err := svc.ListBuckets(nil)
// 	assert.Nil(t, err)
// 	for _, bucket := range results.Buckets {
// 		fmt.Printf("bucket:[%s] created on %s\n",
// 			aws.StringValue(bucket.Name), aws.TimeValue(bucket.CreationDate))
// 	}

// 	params := &s3.ListObjectsInput{
// 		Bucket: aws.String("ckman.backup"),
// 	}
// 	resp, err := svc.ListObjects(params)
// 	assert.Nil(t, err)
// 	for _, item := range resp.Contents {
// 		name := path.Base(*item.Key)
// 		dir := path.Dir(*item.Key)
// 		fmt.Printf("name: %v, dir: %v\n", name, dir)
// 		slot := strings.Split(name, "_")[3] //shard_%d_host_slot.suffix
// 		idx := strings.Index(slot, ".")
// 		slot = slot[:idx]
// 		slotTime, err := time.Parse("20060102150405", slot)
// 		assert.Nil(t, err)
// 		fmt.Println(slotTime)
// 	}
// }
