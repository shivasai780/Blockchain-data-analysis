const AWS = require('aws-sdk');
const { faker } = require('@faker-js/faker');

const kinesis = new AWS.Kinesis({ region: 'ap-south-1' });

const streamName = 'BlkTest';  // Kinesis stream name

// Dictionary to track each NFT's ownership and sale status
const nftStatus = {};

// Function to generate a random NFT creation event
const generateNFTCreationEvent = () => {
  const nftId = faker.string.uuid();
  nftStatus[nftId] = { owner: null, onSale: false };  // Initialize ownership and sale status
  return {
    eventType: 'nftCreation',
    nft_id: nftId,  // Unique NFT ID
    name: faker.commerce.productName(),  // Random product name
    attributes: JSON.stringify({
      color: faker.color.human(),
      pattern: faker.helpers.arrayElement(['striped', 'dotted', 'solid']),
      rarity: faker.helpers.arrayElement(['common', 'rare', 'epic', 'legendary']),
      eyes: faker.helpers.arrayElement(['blue', 'green', 'brown', 'black']),
      material: faker.helpers.arrayElement(['plastic', 'metal', 'wood'])
    })
  };
};

// Function to generate a random NFT transaction event
const generateNFTTransactionEvent = () => {
  const nftId = faker.helpers.arrayElement(Object.keys(nftStatus));

  // Only proceed if NFT exists and is on sale
  if (!nftStatus[nftId].onSale) return null;

  const buyer = faker.internet.userName();
  const seller = nftStatus[nftId].owner;

  return {
    eventType: 'nftTransaction',
    transaction_id: faker.string.uuid(),  // Random transaction ID
    nft_id: nftId,  // NFT being transacted
    seller,
    buyer,
    price: faker.commerce.price(0.01, 1000, 8),
    transaction_type: 'sell',
    timestamp: new Date().toISOString()
  };
};

// Function to send event data to Kinesis stream
const sendEventToKinesis = (event) => {
  if (!event) return;  // Skip if event is null

  const params = {
    Data: JSON.stringify(event),
    PartitionKey: event.nft_id,  // Partition key based on nft_id
    StreamName: streamName
  };

  kinesis.putRecord(params, (err, data) => {
    if (err) {
      console.error('Error sending data to Kinesis:', err);
    } else {
      console.log(`Successfully sent data to Kinesis: ${JSON.stringify(event)}`);
    }
  });
};

// Function to update the ownership and sale status after a transaction
const completeTransaction = (event) => {
  const { nft_id, buyer } = event;
  nftStatus[nft_id] = { owner: buyer, onSale: false };  // Transfer ownership and mark as not on sale
};

// Function to randomly send either NFT creation or transaction event
const sendRandomEvent = () => {
  const hasNFTs = Object.keys(nftStatus).length > 0;
  // Randomly decide whether to send an NFT creation or NFT transaction event
  const isCreationEvent = !hasNFTs || Math.random() < 0.3;  // 30% chance for creation event, 70% for transaction
  
  if (isCreationEvent) {
    // Generate and send an NFT creation event
    const nftCreationEvent = generateNFTCreationEvent();
    nftStatus[nftCreationEvent.nft_id].owner = faker.internet.userName();  // Assign initial owner
    nftStatus[nftCreationEvent.nft_id].onSale = true;  // Put the NFT on sale initially
    sendEventToKinesis(nftCreationEvent);
    setTimeout(sendRandomEvent, 5000);  // Wait 5 seconds before sending another random event
  } 
  else {
    // Generate and send an NFT transaction event
    const nftTransactionEvent = generateNFTTransactionEvent();
    if (nftTransactionEvent) {
      sendEventToKinesis(nftTransactionEvent);
      completeTransaction(nftTransactionEvent);  // Update ownership and sale status
    }
    setTimeout(sendRandomEvent, 1000);  // Wait 1 second before sending another random event
  }
};

// Start sending random events
sendRandomEvent();
