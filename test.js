import  axios from 'axios';
let data = JSON.stringify({
    "key": "name",
    "value": "hadi1"
});

let config = {
    method: 'post',
    maxBodyLength: Infinity,
    url: 'http://localhost:3000/produce',
    headers: {
        'Content-Type': 'application/json'
    },
    data: data
};


let init = 0
while (init < 10000) {
    axios.request(config)
        .then((response) => {
            console.log(JSON.stringify(response.data));
        })
        .catch((error) => {
            console.log(error);
        });
    init++;
}

