<?php

namespace App\Form\DataTransformer;

use BCCoreBundle\Entity\SfAsset;
use BCCoreBundle\Entity\SfAssetFolder;
use Symfony\Component\Form\DataTransformerInterface;
use Symfony\Component\Form\Exception\TransformationFailedException;
use Symfony\Component\HttpFoundation\File\File;
use Symfony\Component\HttpFoundation\File\UploadedFile;


class SfAssetToFileTransformer implements DataTransformerInterface
{
    private string $uploadDir;
    private SfAssetFolder $assetFolder;
    private $uploadFilename;
    private $uploadValidate;
    private bool $multiple;
    private ?SfAsset $asset;

//    private SfAssetFolder $assetFolder;

    public function __construct(
        string $uploadDir,
        SfAssetFolder $assetFolder,
        callable $uploadFilename,
        callable $uploadValidate,
        bool $multiple,
        ?SfAsset $asset = null)
    {
        $this->uploadDir = $uploadDir;
        $this->assetFolder = $assetFolder;
        $this->uploadFilename = $uploadFilename;
        $this->uploadValidate = $uploadValidate;
        $this->multiple = $multiple;
        $this->asset = $asset;
    }

    public function transform(mixed $value): mixed
    {
        if (null === $value || [] === $value) {
            return null;
        }

        if (!$this->multiple) {
            return $this->doTransform($value);
        }

        if (!\is_array($value)) {
            throw new TransformationFailedException('Expected an array or null.');
        }

        return array_map([$this, 'doTransform'], $value);
    }

    public function reverseTransform(mixed $value): mixed
    {
        if (null === $value || [] === $value) {
            return null;
        }

        if (!$this->multiple) {
            return $this->doReverseTransform($value);
        }

        if (!\is_array($value)) {
            throw new TransformationFailedException('Expected an array or null.');
        }

        return array_map([$this, 'doReverseTransform'], $value);
    }

    private function doTransform($value): ?File
    {
        if (null === $value || "" === $value) {
            return null;
        }

        if ($value instanceof File) {
            return $value;
        }

        if (!$value instanceof SfAsset) {
            throw new TransformationFailedException('Expected a SfAsset or null.');
        }

        $fileName = $value->getFilename();

        if (is_file($this->uploadDir . $fileName)) {
            return new File($this->uploadDir . $fileName);
        }

        return null;
    }

    private function doReverseTransform($value): mixed
    {
        if (null === $value) {
            return new SfAsset();
        }

        if(
            get_class($value) == SfAsset::class ||
            get_parent_class($value) == SfAsset::class
        ) {
            //this case is to validate if file is not changed. return File object to not confuse validator
            return $value;
        }

        if ($value instanceof UploadedFile) {
            if (!$value->isValid()) {
                throw new TransformationFailedException($value->getErrorMessage());
            }
            $filename = ($this->uploadFilename)($value);

            $sfAsset = $this->asset??new SfAsset();
            $sfAsset->setFilename(($this->uploadValidate)($filename));
            $sfAsset->setFolder($this->assetFolder);

            return $sfAsset;
        }

        if ($value instanceof File) {
            $sfAsset = $this->asset??new SfAsset();
            $sfAsset->setFilename($value->getFilename());
            $sfAsset->setFolder($this->assetFolder);
            return $sfAsset;
        }

        throw new TransformationFailedException('Expected an instance of File or null.');
    }
}
