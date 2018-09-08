from setuptools import setup

setup(
    name='ebs-pin',
    version='2.0.0',
    scripts=['ebs-pin'],
    packages=['ebspin'],
    url='https://github.com/DomainGroupOSS/ebs-pin.git',
    description='Pin EBS volumes in a multi-az EC2 instance',
    install_requires=[
        'futures<4.0', 'requests< 3.0.0',
        'six<2.0',
        'click < 7.0',
        'click_log < 1.0',
        'sh < 1.13',
        'backoff < 2.0',
        'arrow < 2.0',
        'pyyaml',
        'python-crontab < 3.0',
        'boto3 < 2.0',
        'pip',
        'backoff < 2.0',
    ],
    keywords='ebs ebspin ebs-pin'
)
