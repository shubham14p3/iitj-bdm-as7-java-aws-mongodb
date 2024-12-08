# Big Data Management G23AI2028 AS-7

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]

## Assignment Details

The project was assigned from the course **Big Data Management G23AI2028 AS-7**, Assignment No 7.

## Assignment Task

1. Write the method load() to load the TPC-H customer and orders data into separate
collections (like how it would be stored in a relational model). The data files are in the
data folder.
2. Write the method loadNest() to load the TPC-H customer and order data into a nested
collection called custorders where each document contains the customer information and
all orders for that customer.
3. Write the method query1() that returns the customer name given a customer id using the
customer collection.
4. Write the method query2() that returns the order date for a given order id using the orders
collection.
5. Write the method query2Nest() that returns order date for a given order id using the
custorders collection.
6. Write the method query3() that returns the total number of orders using the orders
collection.
7. Write the method query3Nest() that returns the total number of orders using the
custorders collection.
8. Write the method query4() that returns the top 5 customers based on total order amount
using the customer and orders collections.
9. Write the method query4Nest() that returns the top 5 customers based on total order
amount using the custorders collection.

### Run the code

```
java -cp "bin;lib/*" App
```

### Prerequisites (Minimum)

- AWS RDS DB
- SQL Basic Commands
- VS CODE

### Refereces

- [Oracle VM VirtualBox](https://www.virtualbox.org/)
- [Github](https://github.com)
- [Loom](https://www.loom.com/)

## Authors

👤 **Shubham Raj**

- Github: [@ShubhamRaj](https://github.com/shubham14p3)
- Linkedin: [Shubham14p3](https://www.linkedin.com/in/shubham14p3/)
- Roll No - G23AI2028


## 🤝 Contributing

Contributions, issues and feature requests are welcome!

Feel free to check the [issues page](https://github.com/shubham14p3/vm-g23ai2028-php/issues).

## Show your support

Give a ⭐️ if you like this project!

## Acknowledgments

- Project requested by [IITJ](https://www.iitj.ac.in/).

<!-- MARKDOWN LINKS & IMAGES -->

[contributors-shield]: https://img.shields.io/github/contributors/shubham14p3/members-only.svg?style=flat-square
[contributors-url]: https://github.com/shubham14p3/vm-g23ai2028-php/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/shubham14p3/members-only.svg?style=flat-square
[forks-url]: https://github.com/shubham14p3/vm-g23ai2028-php/network/members
[stars-shield]: https://img.shields.io/github/stars/shubham14p3/members-only.svg?style=flat-square
[stars-url]: https://github.com/shubham14p3/vm-g23ai2028-php/stargazers
[issues-shield]: https://img.shields.io/github/issues/shubham14p3/members-only.svg?style=flat-square
[issues-url]: https://github.com/shubham14p3/vm-g23ai2028-php/issues


